package protostream

import (
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/ddirect/xrand"
)

type streamtest struct {
	rfill, rcheck, rnd *xrand.Xrand
	chunkSizeFactory   func() int
	left               int
}

func newStreamtest() *streamtest {
	a, b := xrand.NewPair()
	return &streamtest{a, b, xrand.New(), nil, 0}
}

func (s *streamtest) Fill(b []byte) (int, error) {
	var err error
	if s.left <= 0 {
		err = io.EOF
		// return err immediately or together with the last chunk of data
		if s.rnd.Bool() {
			return 0, err
		}
	}
	size := s.chunkSizeFactory()
	if size > 0x8000 {
		size = 0x8000
	}
	s.rfill.Fill(b[:size])
	s.left -= size
	return size, err
}

func (s *streamtest) Check(b []byte) error {
	if len(b) == 0 {
		return errors.New("check received empty buffer")
	}
	if s.rcheck.Verify(b) {
		return nil
	}
	return errors.New("check failed")
}

func (s *streamtest) Init(sh int) {
	var minChunk, maxChunk int
	if sh == 0 {
		minChunk, maxChunk = 1, 256
	} else {
		minChunk = 256 << (sh - 1)
		maxChunk = minChunk * 2
	}
	const minSize = 1e6
	s.chunkSizeFactory = s.rnd.UniformFactory(minChunk, maxChunk)
	s.left = s.rnd.Uniform(minSize, minSize*10)
}

func TestStreamSerDes(t *testing.T) {
	rpipe, wpipe := io.Pipe()

	rstream := NewReader(rpipe)
	wstream := NewWriter(wpipe)

	st := newStreamtest()

	// check Init() before changing this
	const transferCount = 9

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for i := 0; i < transferCount; i++ {
			if err := rstream.ReadStream(st.Check); err != nil {
				t.Fatalf("ReadStream: %s", err)
			}
		}
		wg.Done()
	}()

	for i := 0; i < transferCount; i++ {
		st.Init(i)
		if err := wstream.WriteStream(st.Fill); err != nil {
			t.Fatalf("WriteStream: %s", err)
		}
	}
	wstream.Flush()
	wg.Wait()
}
