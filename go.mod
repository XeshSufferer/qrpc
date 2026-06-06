module github.com/XeshSufferer/qrpc

go 1.26.2

require (
	github.com/planetscale/vtprotobuf v0.6.0
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/quic-go/quic-go v0.60.0
	golang.org/x/crypto v0.51.0 // indirect
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
)

replace github.com/quic-go/quic-go v0.60.0 => github.com/XeshSufferer/aquic-go v0.0.0-20260606144617-cffd19d5fb4c
