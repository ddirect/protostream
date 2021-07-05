package protostream

import (
	"hash/crc32"
	"io"

	"google.golang.org/protobuf/proto"
)

const maxSize = 0x8000

var crcTable = crc32.MakeTable(crc32.Castagnoli)
var crcCheck = crc32.Checksum([]byte{0, 0, 0, 0}, crcTable)

type ReadWriter interface {
	ReadStream(func([]byte) error) error
	ReadMessage(proto.Message) error
	WriteStream(func([]byte) (int, error)) error
	WriteMessage(proto.Message) error
	Flush() error
}

func New(rw io.ReadWriter) ReadWriter {
	return &struct {
		*Reader
		*Writer
	}{NewReader(rw), NewWriter(rw)}
}
