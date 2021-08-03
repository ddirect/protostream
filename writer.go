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

func (w *Writer) writeCore(n int, hn int) {
	binary.LittleEndian.PutUint16(w.buf[:], uint16(hn))
	binary.LittleEndian.PutUint32(w.buf[2:][n:], crc32.Checksum(w.buf[2:][:n], crcTable))
	check.IE(w.w.Write(w.buf[:n+6]))
}

func (w *Writer) writeChunk(filler func([]byte) (int, error), loop bool) (errOut error) {
	defer check.Recover(&errOut)
	data := w.buf[2:][:maxSize]
again:
	n, err := filler(data)
	if err != nil {
		if err == io.EOF {
			w.writeCore(n, n)
			// ensure a terminating chunk is written
			if n != 0 {
				w.writeCore(0, 0)
			}
			return
		}
		n = copy(data, err.Error())
		w.writeCore(n, errorPoint+n)
		w.Flush()
		panic(err)
	}
	w.writeCore(n, n)
	if loop {
		goto again
	}
	return
}

func (w *Writer) WriteStream(filler func([]byte) (int, error)) (err error) {
	return w.writeChunk(filler, true)
}

func (w *Writer) WriteMessage(m proto.Message) (err error) {
	return w.writeChunk(func(data []byte) (int, error) {
		res, err := proto.MarshalOptions{}.MarshalAppend(data[:0], m)
		return len(res), err
	}, false)
}
