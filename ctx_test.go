package qrpc

import (
	"sync"
	"testing"

	"github.com/XeshSufferer/qrpc/protos/pb/gen"
)

func TestCtxBody(t *testing.T) {
	req := &gen.Request{Body: []byte("request-body")}
	resp := &gen.Response{Body: []byte("response-body")}
	ctx := NewCtx(req, resp)
	defer ReleaseCtx(ctx)

	if v := string(ctx.Body()); v != "request-body" {
		t.Fatalf("expected request-body, got %v", v)
	}
}

func TestCtxHeaders(t *testing.T) {
	req := &gen.Request{Headers: [][]byte{[]byte("k1"), []byte("v1")}}
	resp := &gen.Response{}
	ctx := NewCtx(req, resp)
	defer ReleaseCtx(ctx)

	h := ctx.Headers()
	if len(h) != 2 || string(h[0]) != "k1" || string(h[1]) != "v1" {
		t.Fatalf("unexpected headers: %v", h)
	}
}

func TestCtxMethod(t *testing.T) {
	req := &gen.Request{Method: []byte("echo")}
	resp := &gen.Response{}
	ctx := NewCtx(req, resp)
	defer ReleaseCtx(ctx)

	if v := string(ctx.Method()); v != "echo" {
		t.Fatalf("expected echo, got %v", v)
	}
}

func TestCtxSetBody(t *testing.T) {
	req := &gen.Request{}
	resp := &gen.Response{}
	ctx := NewCtx(req, resp)
	defer ReleaseCtx(ctx)

	ctx.SetBody([]byte("new-body"))
	if string(resp.Body) != "new-body" {
		t.Fatalf("expected new-body, got %v", string(resp.Body))
	}
}

func TestCtxSetHeaders(t *testing.T) {
	req := &gen.Request{}
	resp := &gen.Response{}
	ctx := NewCtx(req, resp)
	defer ReleaseCtx(ctx)

	headers := [][]byte{[]byte("k1"), []byte("v1")}
	ctx.SetHeaders(headers)
	if len(resp.Headers) != 2 || string(resp.Headers[0]) != "k1" {
		t.Fatalf("unexpected headers: %v", resp.Headers)
	}
}

func TestCtxSetCode(t *testing.T) {
	req := &gen.Request{}
	resp := &gen.Response{}
	ctx := NewCtx(req, resp)
	defer ReleaseCtx(ctx)

	ctx.SetCode(200)
	if resp.Code != 200 {
		t.Fatalf("expected 200, got %d", resp.Code)
	}
}

func TestCtxSetHeader(t *testing.T) {
	req := &gen.Request{}
	resp := &gen.Response{}
	ctx := NewCtx(req, resp)
	defer ReleaseCtx(ctx)

	ctx.SetHeader("key", "value")
	if len(resp.Headers) != 2 || string(resp.Headers[0]) != "key" || string(resp.Headers[1]) != "value" {
		t.Fatalf("unexpected headers: %v", resp.Headers)
	}
}

func TestCtxGetHeader(t *testing.T) {
	req := &gen.Request{Headers: [][]byte{[]byte("key"), []byte("value"), []byte("k2"), []byte("v2")}}
	resp := &gen.Response{}
	ctx := NewCtx(req, resp)
	defer ReleaseCtx(ctx)

	if v := ctx.GetHeader("key", "default"); v != "value" {
		t.Fatalf("expected value, got %v", v)
	}
	if v := ctx.GetHeader("nonexistent", "default"); v != "default" {
		t.Fatalf("expected default, got %v", v)
	}
}

func TestCtxLocals(t *testing.T) {
	req := &gen.Request{}
	resp := &gen.Response{}
	ctx := NewCtx(req, resp)
	defer ReleaseCtx(ctx)

	ctx.Locals().SetString("k", "v")
	if v := ctx.Locals().GetString("k"); v != "v" {
		t.Fatalf("expected v, got %v", v)
	}
}

func TestCtxReleaseReuses(t *testing.T) {
	req1 := &gen.Request{Method: []byte("m1")}
	resp1 := &gen.Response{Code: 100}
	ctx1 := NewCtx(req1, resp1)
	m1 := ctx1.Method()
	ReleaseCtx(ctx1)

	req2 := &gen.Request{Method: []byte("m2")}
	resp2 := &gen.Response{Code: 200}
	ctx2 := NewCtx(req2, resp2)
	if string(ctx2.Method()) != "m2" {
		t.Fatalf("expected m2, got %v", string(ctx2.Method()))
	}
	_ = m1
	ReleaseCtx(ctx2)
}

func TestReqCtxSetMethodBodyHeaders(t *testing.T) {
	req := &gen.Request{}
	rctx := NewReqCtx(req)

	rctx.SetMethod([]byte("test.method"))
	rctx.SetBody([]byte("test-body"))
	rctx.SetHeaders([][]byte{[]byte("h1"), []byte("v1")})

	if string(rctx.Method()) != "test.method" {
		t.Fatalf("expected test.method, got %v", string(rctx.Method()))
	}
	if string(rctx.Body()) != "test-body" {
		t.Fatalf("expected test-body, got %v", string(rctx.Body()))
	}
	h := rctx.Headers()
	if len(h) != 2 || string(h[0]) != "h1" || string(h[1]) != "v1" {
		t.Fatalf("unexpected headers: %v", h)
	}

	ReleaseReqCtx(rctx)
}

func TestReqCtxRequestId(t *testing.T) {
	req := &gen.Request{RequestId: 42}
	rctx := NewReqCtx(req)
	if rctx.RequestId() != 42 {
		t.Fatalf("expected 42, got %d", rctx.RequestId())
	}
	ReleaseReqCtx(rctx)
}

func TestReqCtxReqAccess(t *testing.T) {
	req := &gen.Request{Method: []byte("m"), Body: []byte("b")}
	rctx := NewReqCtx(req)
	if rctx.Req() != req {
		t.Fatal("Req() should return the same pointer")
	}
	ReleaseReqCtx(rctx)
}

func TestReqCtxLocals(t *testing.T) {
	req := &gen.Request{}
	rctx := NewReqCtx(req)
	rctx.Locals().Set("k", 42)
	if v := rctx.Locals().Get("k").(int); v != 42 {
		t.Fatalf("expected 42, got %v", v)
	}
	ReleaseReqCtx(rctx)
}

func TestRespCtxBody(t *testing.T) {
	resp := &gen.Response{Body: []byte("resp-body")}
	r := NewRespCtx(resp)
	if string(r.Body()) != "resp-body" {
		t.Fatalf("expected resp-body, got %v", string(r.Body()))
	}
	ReleaseRespCtx(r)
}

func TestRespCtxCode(t *testing.T) {
	resp := &gen.Response{Code: 200}
	r := NewRespCtx(resp)
	if r.Code() != 200 {
		t.Fatalf("expected 200, got %d", r.Code())
	}
	ReleaseRespCtx(r)
}

func TestRespCtxHeaders(t *testing.T) {
	resp := &gen.Response{Headers: [][]byte{[]byte("k"), []byte("v")}}
	r := NewRespCtx(resp)
	h := r.Headers()
	if len(h) != 2 || string(h[0]) != "k" || string(h[1]) != "v" {
		t.Fatalf("unexpected headers: %v", h)
	}
	ReleaseRespCtx(r)
}

func TestRespCtxRequestId(t *testing.T) {
	resp := &gen.Response{RequestId: 7}
	r := NewRespCtx(resp)
	if r.RequestId() != 7 {
		t.Fatalf("expected 7, got %d", r.RequestId())
	}
	ReleaseRespCtx(r)
}

func TestRespCtxRespAccess(t *testing.T) {
	resp := &gen.Response{Code: 200}
	r := NewRespCtx(resp)
	if r.Resp() != resp {
		t.Fatal("Resp() should return the same pointer")
	}
	ReleaseRespCtx(r)
}

func TestCtxPoolReuse(t *testing.T) {
	req := &gen.Request{Method: []byte("m1")}
	resp := &gen.Response{Code: 100}
	c1 := NewCtx(req, resp)
	ReleaseCtx(c1)

	req2 := &gen.Request{Method: []byte("m2")}
	resp2 := &gen.Response{Code: 200}
	c2 := NewCtx(req2, resp2)
	if string(c2.Method()) != "m2" || c2.resp.Code != 200 {
		t.Fatal("reused ctx should have new values")
	}
	ReleaseCtx(c2)
}

func TestEventCtxInterface(t *testing.T) {
	req := &gen.Request{Body: []byte("b"), Method: []byte("m"), Headers: [][]byte{[]byte("k"), []byte("v")}}
	resp := &gen.Response{}
	ctx := NewCtx(req, resp)
	defer ReleaseCtx(ctx)

	var ectx EventCtx = ctx
	if string(ectx.Body()) != "b" {
		t.Fatal("EventCtx Body mismatch")
	}
	if string(ectx.Method()) != "m" {
		t.Fatal("EventCtx Method mismatch")
	}
}

func TestLocalsSetStringGetString(t *testing.T) {
	l := NewLocals().(*LocalsImpl)
	l.SetString("key1", "val1")
	l.SetString("key2", "val2")

	if v := l.GetString("key1"); v != "val1" {
		t.Fatalf("expected val1, got %v", v)
	}
	if v := l.GetString("key2"); v != "val2" {
		t.Fatalf("expected val2, got %v", v)
	}
	if v := l.GetString("nonexistent"); v != "" {
		t.Fatalf("expected empty, got %v", v)
	}
}

func TestLocalsSetGet(t *testing.T) {
	l := NewLocals().(*LocalsImpl)
	l.Set("int", 42)
	l.Set("str", "hello")
	l.Set("struct", struct{ X int }{X: 1})

	if v := l.Get("int").(int); v != 42 {
		t.Fatalf("expected 42, got %v", v)
	}
	if v := l.Get("str").(string); v != "hello" {
		t.Fatalf("expected hello, got %v", v)
	}
	if v := l.Get("nonexistent"); v != nil {
		t.Fatalf("expected nil, got %v", v)
	}
}

func TestLocalsReset(t *testing.T) {
	l := NewLocals().(*LocalsImpl)
	l.SetString("k1", "v1")
	l.Set("k2", 42)
	l.Reset()

	if v := l.GetString("k1"); v != "" {
		t.Fatalf("expected empty after reset, got %v", v)
	}
	if v := l.Get("k2"); v != nil {
		t.Fatalf("expected nil after reset, got %v", v)
	}
}

func TestLocalsOverwrite(t *testing.T) {
	l := NewLocals().(*LocalsImpl)
	l.SetString("key", "old")
	l.SetString("key", "new")
	if v := l.GetString("key"); v != "new" {
		t.Fatalf("expected new, got %v", v)
	}

	l.Set("key", 1)
	l.Set("key", 2)
	if v := l.Get("key").(int); v != 2 {
		t.Fatalf("expected 2, got %v", v)
	}
}

func TestLocalsConcurrent(t *testing.T) {
	l := NewLocals().(*LocalsImpl)
	var wg sync.WaitGroup
	n := 100

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := string(rune('a' + i))
			l.SetString(key, key)
			l.GetString(key)
			l.Set(key, i)
			l.Get(key)
		}(i)
	}
	wg.Wait()
}

func TestLocalsResetConcurrent(t *testing.T) {
	l := NewLocals().(*LocalsImpl)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				l.SetString("k", "v")
				l.Set("k", j)
				l.Reset()
			}
		}()
	}
	wg.Wait()
}
