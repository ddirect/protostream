package protostream

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/ddirect/check"
	"google.golang.org/protobuf/proto"
)

type Reader struct {
	r        *bufio.Reader
	nextSize int
	buf      [maxSize + 6]byte
}

func NewReader(r io.Reader) *Reader {
	return &Reader{r: bufio.NewReader(r), nextSize: -1}
}

func (r *Reader) readChunk() []byte {
	size := r.nextSize
	r.nextSize = -1
	if size < 0 {
		buf := r.buf[:2]
		check.IE(io.ReadFull(r.r, buf))
		size = int(binary.LittleEndian.Uint16(buf))
	}
	isErr := false
	if size >= errorPoint {
		size -= errorPoint
		isErr = true
	}
	// read data + crc + (optionally) next size
	n := check.IE(io.ReadAtLeast(r.r, r.buf[:size+6], size+4))
	switch n {
	case size + 5:
		check.E(r.r.UnreadByte())
	case size + 6:
		r.nextSize = int(binary.LittleEndian.Uint16(r.buf[size+4:]))
	}
	if crc32.Checksum(r.buf[:size+4], crcTable) != crcCheck {
		panic(errors.New("CRC check failed"))
	}
	if isErr {
		panic(fmt.Errorf("remote error: %s", r.buf[:size]))
	}
	return r.buf[:size]
}

func (r *Reader) ReadStream(chunkHandler func([]byte) error) (err error) {
	defer check.Recover(&err)
	for {
		data := r.readChunk()
		if len(data) == 0 {
			return
		}
		check.E(chunkHandler(data))
	}
}

func (r *Reader) ReadMessage(m proto.Message) (err error) {
	defer check.Recover(&err)
	check.E(proto.Unmarshal(r.readChunk(), m))
	return
}
