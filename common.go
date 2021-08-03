package protostream

import (
	"hash/crc32"
	"io"
)

const maxSize = 0x8000
const errorPoint = maxSize + 1

var crcTable = crc32.MakeTable(crc32.Castagnoli)
var crcCheck = crc32.Checksum([]byte{0, 0, 0, 0}, crcTable)

type ReadWriter struct {
	*Reader
	*Writer
}

func New(rw io.ReadWriter) ReadWriter {
	return ReadWriter{NewReader(rw), NewWriter(rw)}
}
