package internal

import (
	"io"
	"sync"
	"unsafe"

	"google.golang.org/protobuf/proto"
)

type buff struct {
	buff []byte
}

var (
	bufferPool = sync.Pool{
		New: func() any {
			return &buff{buff: make([]byte, 0, 1024)}
		},
	}
)

func MarshalToWriter(w io.Writer, m proto.Message) (int, error) {
	pBuf := bufferPool.Get().(*buff)
	pBuf.buff = pBuf.buff[:0]
	defer bufferPool.Put(pBuf)

	options := proto.MarshalOptions{}

	data, err := options.MarshalAppend(pBuf.buff, m)
	if err != nil {
		return 0, err
	}

	if len(data) != len(pBuf.buff) {
		pBuf.buff = data
	}

	return w.Write(data)
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
