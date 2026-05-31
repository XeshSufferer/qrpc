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

	"github.com/XeshSufferer/qrpc/protos/pb/gen"
	"github.com/quic-go/quic-go"
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

func startTestServer(b *testing.B, handler func(*gen.Request) *gen.Response) (QRpcServer, string) {
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

	handler := func(req *gen.Request) *gen.Response {
		return &gen.Response{
			RequestId: req.RequestId,
			Code:      200,
			Body:      req.Body,
		}
	}

	_, addr := startTestServer(b, handler)

	client, err := NewClient(context.Background(), addr, getTestTLSConfig())
	if err != nil {
		b.Fatal(err)
	}

	time.Sleep(900 * time.Millisecond)

	return client
}

func BenchmarkRoundTrip_16B(b *testing.B) {
	client := setupRoundTrip(b, 16)
	method := []byte("echo")
	body := make([]byte, 16)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := client.SendRequest(context.Background(), method, body, nil)
		if err != nil {
			b.Fatal(err)
		}
		if resp.Code != 200 {
			b.Fatal("unexpected status code")
		}
	}
}

func BenchmarkRoundTrip_1KB(b *testing.B) {
	client := setupRoundTrip(b, 1024)
	method := []byte("echo")
	body := make([]byte, 1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := client.SendRequest(context.Background(), method, body, nil)
		if err != nil {
			b.Fatal(err)
		}
		if resp.Code != 200 {
			b.Fatal("unexpected status code")
		}
	}
}

func BenchmarkRoundTrip_4KB(b *testing.B) {
	client := setupRoundTrip(b, 4096)
	method := []byte("echo")
	body := make([]byte, 4096)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := client.SendRequest(context.Background(), method, body, nil)
		if err != nil {
			b.Fatal(err)
		}
		if resp.Code != 200 {
			b.Fatal("unexpected status code")
		}
	}
}

func BenchmarkRoundTripParallel(b *testing.B) {
	client := setupRoundTrip(b, 256)
	method := []byte("echo")
	body := make([]byte, 256)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := client.SendRequest(context.Background(), method, body, nil)
			if err != nil {
				b.Fatal(err)
			}
			if resp.Code != 200 {
				b.Fatal("unexpected status code")
			}
		}
	})
}

func BenchmarkRoundTrip_1KB_WithStartedServer(b *testing.B) {
	client, _ := NewClient(context.Background(), "localhost:8081", getTestTLSConfig())
	method := []byte("log")
	body := make([]byte, 1024)
	time.Sleep(time.Millisecond * 500)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := client.SendRequest(context.Background(), method, body, nil)
		if err != nil {
			b.Fatal(err)
		}
		if resp.Code != 200 {
			b.Fatal("unexpected status code")
		}
	}
}
