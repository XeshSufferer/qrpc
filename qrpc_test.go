package qrpc

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"log/slog"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	quic "github.com/XeshSufferer/aquic-go"
)

func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	m.Run()
}

var (
	tlsConfigOnce sync.Once
	testTLSConfig *tls.Config
)

func getTestTLSConfig() *tls.Config {
	tlsConfigOnce.Do(func() {
		priv, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			panic(err)
		}
		template := x509.Certificate{
			SerialNumber:          big.NewInt(1),
			Subject:               pkix.Name{Organization: []string{"Test"}},
			NotBefore:             time.Now(),
			NotAfter:              time.Now().Add(24 * time.Hour),
			KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
			ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
			BasicConstraintsValid: true,
			DNSNames:              []string{"localhost"},
		}
		derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
		if err != nil {
			panic(err)
		}
		certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
		privBytes, err := x509.MarshalPKCS8PrivateKey(priv)
		if err != nil {
			panic(err)
		}
		keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privBytes})
		cert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			panic(err)
		}
		testTLSConfig = &tls.Config{
			Certificates:       []tls.Certificate{cert},
			InsecureSkipVerify: true,
			NextProtos:         []string{"qrpc"},
			MinVersion:         tls.VersionTLS13,
		}
	})
	return testTLSConfig
}

func startTestServer(b *testing.B, handler func(Ctx)) (QRpcServer, string) {
	b.Helper()

	udpAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		b.Fatal(err)
	}

	quicCfg := &quic.Config{KeepAlivePeriod: 15 * time.Second}
	listener, err := quic.Listen(udpConn, getTestTLSConfig(), quicCfg)
	if err != nil {
		b.Fatal(err)
	}

	addr := listener.Addr().String()

	server := newServer(listener)
	server.AddHandler("echo", handler)
	go server.startListen()

	b.Cleanup(func() {
		listener.Close()
	})

	return server, addr
}

func setupRoundTrip(b *testing.B, bodySize int) Client {
	b.Helper()

	handler := func(c Ctx) {
		c.SetBody(c.Body())
		c.SetCode(200)
		c.SetHeaders(c.Headers())
	}

	_, addr := startTestServer(b, handler)

	client, err := NewClient(context.Background(), addr, getTestTLSConfig(), 1)
	if err != nil {
		b.Fatal(err)
	}

	time.Sleep(900 * time.Millisecond)

	return client
}

func benchSend(client Client, method []byte, body []byte) {
	req := client.NewRequest()
	req.SetMethod(method)
	req.SetBody(body)
	resp, err := client.SendRequest(context.Background(), req)
	if err != nil {
		panic(err)
	}
	if resp.Code() != 200 {
		panic("unexpected status code")
	}
	client.ReleaseResponse(resp)
}

func BenchmarkRoundTrip_16B(b *testing.B) {
	client := setupRoundTrip(b, 16)
	method := []byte("echo")
	body := make([]byte, 16)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchSend(client, method, body)
	}
}

func BenchmarkRoundTrip_1KB(b *testing.B) {
	client := setupRoundTrip(b, 1024)
	method := []byte("echo")
	body := make([]byte, 1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchSend(client, method, body)
	}
}

func BenchmarkRoundTrip_4KB(b *testing.B) {
	client := setupRoundTrip(b, 4096)
	method := []byte("echo")
	body := make([]byte, 4096)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchSend(client, method, body)
	}
}

func BenchmarkRoundTripParallel(b *testing.B) {
	client := setupRoundTrip(b, 256)
	method := []byte("echo")
	body := make([]byte, 256)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			benchSend(client, method, body)
		}
	})
}

func BenchmarkRoundTrip_1KB_WithStartedServer(b *testing.B) {
	client, _ := NewClient(context.Background(), "localhost:8081", getTestTLSConfig(), 1)
	method := []byte("echo")
	body := make([]byte, 1024)
	time.Sleep(time.Millisecond * 500)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchSend(client, method, body)
	}
}

func BenchmarkRoundTrip_4KB_WithStartedServer(b *testing.B) {
	client, _ := NewClient(context.Background(), "localhost:8081", getTestTLSConfig(), 1)
	method := []byte("echo")
	body := make([]byte, 1024*4)
	time.Sleep(time.Millisecond * 500)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchSend(client, method, body)
	}
}

func BenchmarkRoundTrip_16KB_WithStartedServer(b *testing.B) {
	client, _ := NewClient(context.Background(), "localhost:8081", getTestTLSConfig(), 1)
	method := []byte("echo")
	body := make([]byte, 1024*16)
	time.Sleep(time.Millisecond * 500)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchSend(client, method, body)
	}
}
