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
	r              *bufio.Reader
	nextSizeOffset int
	buf            [maxSize + 8]byte // max data + length + crc + next length
}

func NewReader(r io.Reader) *Reader {
	return &Reader{r: bufio.NewReader(r), nextSizeOffset: -1}
}

func (r *Reader) readChunk() []byte {
	sizeBuf := r.buf[:2]
	if r.nextSizeOffset < 0 {
		check.IE(io.ReadFull(r.r, sizeBuf))
	} else {
		binary.LittleEndian.PutUint16(sizeBuf, binary.LittleEndian.Uint16(r.buf[r.nextSizeOffset:]))
		r.nextSizeOffset = -1
	}
	size := int(binary.LittleEndian.Uint16(sizeBuf))
	isErr := false
	if size >= errorPoint {
		size -= errorPoint
		isErr = true
	}
	// read data + crc + (optionally) next size
	n := check.IE(io.ReadAtLeast(r.r, r.buf[2:][:size+6], size+4))
	switch n {
	case size + 5:
		check.E(r.r.UnreadByte())
	case size + 6:
		r.nextSizeOffset = size + 6
	}
	if crc32.Checksum(r.buf[:size+6], crcTable) != crcCheck {
		panic(errors.New("CRC check failed"))
	}
	if isErr {
		panic(fmt.Errorf("remote error: %s", r.buf[2:][:size]))
	}
	return r.buf[2:][:size]
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
