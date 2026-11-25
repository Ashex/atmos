package repo

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/mst"
)

// ErrCARSpillQuota indicates that the next block would exceed the shared
// temporary block budget. Release other stores or increase the budget to retry.
var ErrCARSpillQuota = errors.New("repo: CAR spill quota exhausted")

// CARSpillBudget caps the total logical block bytes written or buffered across
// live temporary stores that share it. It is safe for concurrent use.
type CARSpillBudget struct {
	limit uint64
	used  atomic.Uint64
}

// NewCARSpillBudget sets a shared byte limit. Zero disables accounting.
func NewCARSpillBudget(limit uint64) *CARSpillBudget {
	if limit == 0 {
		return nil
	}
	return &CARSpillBudget{limit: limit}
}

func (b *CARSpillBudget) reserve(n uint64) error {
	if b == nil {
		return nil
	}
	for {
		used := b.used.Load()
		if n > b.limit-used {
			return fmt.Errorf("%w: need %d bytes, %d available", ErrCARSpillQuota, n, b.limit-used)
		}
		if b.used.CompareAndSwap(used, used+n) {
			return nil
		}
	}
}

func (b *CARSpillBudget) release(n uint64) {
	if b != nil {
		b.used.Add(-n)
	}
}

type diskBlockLocation struct {
	offset int64
	size   int
}

type diskBlockStore struct {
	file   *os.File
	budget *CARSpillBudget
	writer *bufio.Writer
	index  map[cbor.CID]diskBlockLocation
	end    int64
}

func newDiskBlockStore(budget *CARSpillBudget) (*diskBlockStore, error) {
	file, err := openCARSpillFile()
	if err != nil {
		return nil, fmt.Errorf("repo: creating temporary block store: %w", err)
	}
	return &diskBlockStore{
		file:   file,
		budget: budget,
		writer: bufio.NewWriterSize(file, 64*1024),
		index:  make(map[cbor.CID]diskBlockLocation),
	}, nil
}

func (s *diskBlockStore) PutBlock(cid cbor.CID, data []byte) error {
	if s.file == nil {
		return os.ErrClosed
	}
	// The CAR reader has verified the CID even for repeated blocks.
	if _, exists := s.index[cid]; exists {
		return nil
	}
	if err := s.budget.reserve(uint64(len(data))); err != nil {
		return err
	}
	loc := diskBlockLocation{offset: s.end, size: len(data)}
	// Keep the reservation until close even if the write only partly succeeds.
	s.end += int64(len(data))
	if _, err := s.writer.Write(data); err != nil {
		return fmt.Errorf("write temporary block: %w", err)
	}
	s.index[cid] = loc
	return nil
}

func (s *diskBlockStore) GetBlock(cid cbor.CID) ([]byte, error) {
	if s.file == nil {
		return nil, os.ErrClosed
	}
	if err := s.writer.Flush(); err != nil {
		return nil, fmt.Errorf("flush temporary blocks: %w", err)
	}
	loc, ok := s.index[cid]
	if !ok {
		return nil, fmt.Errorf("%w: %s", mst.ErrBlockNotFound, cid.String())
	}
	// Returned bytes can outlive the store (e.g. async verifier delivery).
	data := make([]byte, loc.size)
	if _, err := s.file.ReadAt(data, loc.offset); err != nil {
		return nil, fmt.Errorf("read temporary block %s: %w", cid, err)
	}
	return data, nil
}

func (s *diskBlockStore) Close() error {
	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.budget.release(uint64(s.end))
	s.file = nil
	s.writer = nil
	s.index = nil
	if err != nil {
		return fmt.Errorf("close temporary block store: %w", err)
	}
	return nil
}
