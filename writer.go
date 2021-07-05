package protostream

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"

	"github.com/ddirect/check"
	"google.golang.org/protobuf/proto"
)

type Writer struct {
	w   *bufio.Writer
	buf [maxSize + 6]byte
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{w: bufio.NewWriter(w)}
}

func (w *Writer) Flush() error {
	return w.w.Flush()
}

func (w *Writer) writeChunk(filler func([]byte) (int, error)) int {
	n := check.IE(filler(w.buf[2:][:maxSize]))
	binary.LittleEndian.PutUint16(w.buf[:], uint16(n))
	binary.LittleEndian.PutUint32(w.buf[2:][n:], crc32.Checksum(w.buf[2:][:n], crcTable))
	check.IE(w.w.Write(w.buf[:n+6]))
	return n
}

func (w *Writer) WriteStream(filler func([]byte) (int, error)) (err error) {
	defer check.Recover(&err)
	for w.writeChunk(filler) > 0 {
	}
	return
}

func (w *Writer) WriteMessage(m proto.Message) (err error) {
	defer check.Recover(&err)
	w.writeChunk(func(data []byte) (int, error) {
		res, err := proto.MarshalOptions{}.MarshalAppend(data[:0], m)
		return len(res), err
	})
	return
}
