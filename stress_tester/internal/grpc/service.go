package grpc

import (
	"context"

	"github.com/XeshSufferer/qrpc/protos/pb/gen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const ServiceName = "bench.BenchService"

var benchServiceDesc = &grpc.ServiceDesc{
	ServiceName: ServiceName,
	HandlerType: (*BenchServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Call",
			Handler:    _BenchService_Call_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "bench_service.proto",
}

type BenchServiceServer interface {
	Call(context.Context, *gen.Request) (*gen.Response, error)
	mustEmbedUnimplementedBenchServiceServer()
}

type UnimplementedBenchServiceServer struct{}

func (UnimplementedBenchServiceServer) Call(context.Context, *gen.Request) (*gen.Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Call not implemented")
}
func (UnimplementedBenchServiceServer) mustEmbedUnimplementedBenchServiceServer() {}

func RegisterBenchServiceServer(s grpc.ServiceRegistrar, srv BenchServiceServer) {
	s.RegisterService(benchServiceDesc, srv)
}

func _BenchService_Call_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(gen.Request)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BenchServiceServer).Call(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/bench.BenchService/Call",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BenchServiceServer).Call(ctx, req.(*gen.Request))
	}
	return interceptor(ctx, in, info, handler)
}

type BenchServiceClient interface {
	Call(ctx context.Context, in *gen.Request, opts ...grpc.CallOption) (*gen.Response, error)
}

type benchServiceClientImpl struct {
	cc grpc.ClientConnInterface
}

func NewBenchServiceClient(cc grpc.ClientConnInterface) BenchServiceClient {
	return &benchServiceClientImpl{cc}
}

func (c *benchServiceClientImpl) Call(ctx context.Context, in *gen.Request, opts ...grpc.CallOption) (*gen.Response, error) {
	out := new(gen.Response)
	if err := c.cc.Invoke(ctx, "/bench.BenchService/Call", in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}
