package client

import (
	"sync"
	"testing"

	"github.com/XeshSufferer/qrpc/protos/pb/gen"
)

func TestRequestPoolGetAndRelease(t *testing.T) {
	req := GetRequest()
	if req == nil {
		t.Fatal("GetRequest returned nil")
	}

	req.RequestId = 42
	req.Method = []byte("test")
	req.Body = []byte("body")
	req.Headers = [][]byte{[]byte("k"), []byte("v")}

	ReleaseRequest(req)
	if req.RequestId != 0 {
		t.Fatal("ReleaseRequest should reset RequestId")
	}
	if len(req.Method) != 0 {
		t.Fatal("ReleaseRequest should clear Method")
	}
	if len(req.Body) != 0 {
		t.Fatal("ReleaseRequest should clear Body")
	}
}

func TestResponsePoolGetAndRelease(t *testing.T) {
	resp := GetResponse()
	if resp == nil {
		t.Fatal("GetResponse returned nil")
	}

	resp.RequestId = 7
	resp.Code = 200
	resp.Body = []byte("response")
	resp.Headers = [][]byte{[]byte("k"), []byte("v")}

	ReleaseResponse(resp)
	if resp.RequestId != 0 {
		t.Fatal("ReleaseResponse should reset RequestId")
	}
	if resp.Code != 0 {
		t.Fatal("ReleaseResponse should reset Code")
	}
	if len(resp.Body) != 0 {
		t.Fatal("ReleaseResponse should clear Body")
	}
}

func TestRequestPoolReuse(t *testing.T) {
	req1 := GetRequest()
	req1.Method = []byte("original")
	ReleaseRequest(req1)

	req2 := GetRequest()
	if string(req2.Method) != "" {
		t.Fatal("reused request should have empty Method")
	}
	ReleaseRequest(req2)
}

func TestResponsePoolReuse(t *testing.T) {
	resp1 := GetResponse()
	resp1.Code = 999
	ReleaseResponse(resp1)

	resp2 := GetResponse()
	if resp2.Code != 0 {
		t.Fatal("reused response should have zero Code")
	}
	ReleaseResponse(resp2)
}

func TestRequestPoolPreserveCapacity(t *testing.T) {
	req := GetRequest()
	req.Body = make([]byte, 1000)
	req.Headers = make([][]byte, 10)
	capBody := cap(req.Body)
	capHeaders := cap(req.Headers)

	ReleaseRequest(req)

	if cap(req.Body) != capBody {
		t.Fatal("ReleaseRequest should preserve Body capacity")
	}
	if cap(req.Headers) != capHeaders {
		t.Fatal("ReleaseRequest should preserve Headers capacity")
	}
}

func TestResponsePoolPreserveCapacity(t *testing.T) {
	resp := GetResponse()
	resp.Body = make([]byte, 1000)
	resp.Headers = make([][]byte, 10)
	capBody := cap(resp.Body)
	capHeaders := cap(resp.Headers)

	ReleaseResponse(resp)

	if cap(resp.Body) != capBody {
		t.Fatal("ReleaseResponse should preserve Body capacity")
	}
	if cap(resp.Headers) != capHeaders {
		t.Fatal("ReleaseResponse should preserve Headers capacity")
	}
}

func TestRequestPoolResetFields(t *testing.T) {
	req := &gen.Request{
		RequestId: 1,
		Method:    []byte("m"),
		Body:      []byte("b"),
		Headers:   [][]byte{{1}, {2}},
	}
	ReleaseRequest(req)

	if req.RequestId != 0 {
		t.Fatal("RequestId should be 0")
	}
	if len(req.Method) != 0 {
		t.Fatal("Method should be empty")
	}
	if len(req.Body) != 0 {
		t.Fatal("Body should be empty")
	}
	if len(req.Headers) != 0 {
		t.Fatal("Headers should be empty")
	}
}

func TestResponsePoolResetFields(t *testing.T) {
	resp := &gen.Response{
		RequestId: 1,
		Code:      200,
		Method:    []byte("m"),
		Body:      []byte("b"),
		Headers:   [][]byte{{1}, {2}},
	}
	ReleaseResponse(resp)

	if resp.RequestId != 0 {
		t.Fatal("RequestId should be 0")
	}
	if resp.Code != 0 {
		t.Fatal("Code should be 0")
	}
	if len(resp.Method) != 0 {
		t.Fatal("Method should be empty")
	}
	if len(resp.Body) != 0 {
		t.Fatal("Body should be empty")
	}
	if len(resp.Headers) != 0 {
		t.Fatal("Headers should be empty")
	}
}

func TestRequestPoolConcurrent(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := GetRequest()
			req.RequestId = 1
			req.Method = []byte("test")
			ReleaseRequest(req)
		}()
	}
	wg.Wait()
}

func TestResponsePoolConcurrent(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp := GetResponse()
			resp.Code = 200
			ReleaseResponse(resp)
		}()
	}
	wg.Wait()
}

func TestIsTimeoutErrNil(t *testing.T) {
	if IsTimeoutErr(nil) {
		t.Fatal("IsTimeoutErr(nil) should be false")
	}
}
