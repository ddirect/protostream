package protostream

import (
	"bufio"
	"io"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

type Reader struct {
	buf               *bufio.Reader
	sizeVarintMaxSize int // size of a varint representing maxSize
}

const maxVarint64Size = 10 // see protowire.SizeVarint(math.MaxUint64)

func NewReader(r io.Reader, maxSize int) *Reader {
	return &Reader{bufio.NewReaderSize(r, maxSize), int(protowire.SizeVarint(uint64(maxSize)))}
}

func (r *Reader) readVarintCore(maxSize int) (uint64, error) {
	b, err := r.buf.Peek(maxSize)
	if err != nil && err != io.EOF {
		return 0, err
	}
	res, n := protowire.ConsumeVarint(b)
	if n < 0 {
		return 0, protowire.ParseError(n)
	}
	// this is guaranteed to succeed since we are discarding no more than the peeked size
	r.buf.Discard(n)
	return res, nil
}

func (r *Reader) ReadVarint() (uint64, error) {
	return r.readVarintCore(maxVarint64Size)
}

func (r *Reader) ReadProto(m proto.Message) error {
	size, err := r.readVarintCore(r.sizeVarintMaxSize)
	if err != nil {
		return err
	}
	data, err := r.buf.Peek(int(size))
	if err != nil {
		return err
	}
	defer r.buf.Discard(len(data))
	return proto.Unmarshal(data, m)
}

func (r *Reader) ReadData(chunkHandler func([]byte) error) error {
	size, err := r.ReadVarint()
	if err != nil {
		return err
	}
	//	todo :=
}
