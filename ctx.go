package qrpc

import (
	"bytes"
	"sync"
	"unsafe"

	"github.com/XeshSufferer/qrpc/protos/pb/gen"
)

type Locals interface {
	SetString(key, value string)
	GetString(key string) string
	Set(key string, value any)
	Get(key string) any
	Reset()
}

type LocalsImpl struct {
	anyes   map[string]any
	strings map[string]string
	mu      sync.RWMutex
}

func NewLocals() Locals {
	return &LocalsImpl{
		anyes:   map[string]any{},
		strings: map[string]string{},
		mu:      sync.RWMutex{},
	}
}

func (l *LocalsImpl) SetString(key, value string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.strings[key] = value
}

func (l *LocalsImpl) GetString(key string) string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.strings[key]
}

func (l *LocalsImpl) Get(key string) any {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.anyes[key]
}

func (l *LocalsImpl) Set(key string, value any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.anyes[key] = value
}

func (l *LocalsImpl) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	clear(l.anyes)
	clear(l.strings)
}

type Ctx interface {
	Locals() Locals
	Body() []byte
	Headers() [][]byte
	Method() []byte
	SetBody(buff []byte)
	SetHeaders(buff [][]byte)
	GetHeader(key, defaultValue string) string
	SetHeader(key, value string)
	SetCode(code uint32)
}

type EventCtx interface {
	Locals() Locals
	Body() []byte
	Headers() [][]byte
	GetHeader(key, defaultValue string) string
	Method() []byte
}

type CtxImpl struct {
	req    *gen.Request
	resp   *gen.Response
	locals Locals
}

var ctxPool = sync.Pool{
	New: func() any {
		return &CtxImpl{}
	},
}

func NewCtx(req *gen.Request, resp *gen.Response) *CtxImpl {
	ctx := ctxPool.Get().(*CtxImpl)

	ctx.req = req
	ctx.resp = resp

	if ctx.locals == nil {
		ctx.locals = NewLocals()
	}

	return ctx
}

func ReleaseCtx(ctx *CtxImpl) {
	ctx.req = nil
	ctx.resp = nil
	ctx.locals.Reset()
	ctxPool.Put(ctx)
}

func (c *CtxImpl) Locals() Locals {
	return c.locals
}

// REQUEST

func (c *CtxImpl) Body() []byte {
	return c.req.Body
}

func (c *CtxImpl) Headers() [][]byte {
	return c.req.Headers
}

func (c *CtxImpl) Method() []byte {
	return c.req.Method
}

func (c *CtxImpl) GetHeader(key, defaultValue string) string {

	bytesKey := StringToBytes(key)

	for i := 0; i < len(c.req.Headers); i += 2 {
		if bytes.Equal(c.req.Headers[i], bytesKey) {
			return BytesToString(c.req.Headers[i+1])
		}
	}

	return defaultValue
}

// RESPONSE

func (c *CtxImpl) SetBody(buff []byte) {
	c.resp.Body = buff
}

func (c *CtxImpl) SetHeaders(buff [][]byte) {
	c.resp.Headers = buff
}

func (c *CtxImpl) SetCode(code uint32) {
	c.resp.Code = code
}

func (c *CtxImpl) SetHeader(key, value string) {
	c.resp.Headers = append(c.resp.Headers, StringToBytes(key), StringToBytes(value))
}

type ReqCtx interface {
	Locals() Locals
	Body() []byte
	SetBody([]byte)
	Headers() [][]byte
	SetHeaders([][]byte)
	Method() []byte
	SetMethod([]byte)
	RequestId() uint64
}

type RespCtx interface {
	Body() []byte
	Headers() [][]byte
	Code() uint32
	RequestId() uint64
}

type ReqCtxImpl struct {
	req    *gen.Request
	locals Locals
}

var reqCtxPool = sync.Pool{
	New: func() any {
		return &ReqCtxImpl{}
	},
}

func NewReqCtx(req *gen.Request) *ReqCtxImpl {
	ctx := reqCtxPool.Get().(*ReqCtxImpl)
	ctx.req = req

	if ctx.locals == nil {
		ctx.locals = NewLocals()
	}

	return ctx
}

func ReleaseReqCtx(ctx *ReqCtxImpl) {
	ctx.req = nil
	ctx.locals.Reset()
	reqCtxPool.Put(ctx)
}

func (c *ReqCtxImpl) Locals() Locals {
	return c.locals
}

func (c *ReqCtxImpl) Body() []byte {
	return c.req.Body
}

func (c *ReqCtxImpl) SetBody(b []byte) {
	c.req.Body = b
}

func (c *ReqCtxImpl) Headers() [][]byte {
	return c.req.Headers
}

func (c *ReqCtxImpl) SetHeaders(h [][]byte) {
	c.req.Headers = h
}

func (c *ReqCtxImpl) Method() []byte {
	return c.req.Method
}

func (c *ReqCtxImpl) SetMethod(m []byte) {
	c.req.Method = m
}

func (c *ReqCtxImpl) RequestId() uint64 {
	return c.req.RequestId
}

func (c *ReqCtxImpl) Req() *gen.Request {
	return c.req
}

type RespCtxImpl struct {
	resp *gen.Response
}

var respCtxPool = sync.Pool{
	New: func() any {
		return &RespCtxImpl{}
	},
}

func NewRespCtx(resp *gen.Response) *RespCtxImpl {
	ctx := respCtxPool.Get().(*RespCtxImpl)
	ctx.resp = resp
	return ctx
}

func ReleaseRespCtx(ctx *RespCtxImpl) {
	ctx.resp = nil
	respCtxPool.Put(ctx)
}

func (c *RespCtxImpl) Body() []byte {
	return c.resp.Body
}

func (c *RespCtxImpl) Headers() [][]byte {
	return c.resp.Headers
}

func (c *RespCtxImpl) Code() uint32 {
	return c.resp.Code
}

func (c *RespCtxImpl) RequestId() uint64 {
	return c.resp.RequestId
}

func (c *RespCtxImpl) Resp() *gen.Response {
	return c.resp
}

func BytesToString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}

func StringToBytes(s string) []byte {
	if len(s) == 0 {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
