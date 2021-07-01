package protostream

import (
	"bufio"
	"io"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

type Writer struct {
	buf *bufio.Writer
}

func NewWriter(w io.Writer, maxSize int) *Writer {
	return &Writer{bufio.NewWriterSize()}
}

func (w *Writer) WriteVarint(x uint64) error {
	w.buf.Write()
}
