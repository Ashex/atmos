package repo

import (
	"bytes"
	"io"
	"os"
	"slices"
	"testing"

	"github.com/jcalabro/atmos/car"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/mst"
	"github.com/stretchr/testify/require"
)

func TestLoadFromCAROnDisk_SignedRepo(t *testing.T) {
	t.Parallel()

	original, key := buildTestRepo(t, 256)
	var archive bytes.Buffer
	require.NoError(t, original.ExportCAR(&archive, key))
	header, blocks, err := car.ReadAll(bytes.NewReader(archive.Bytes()))
	require.NoError(t, err)

	// Move the commit to the end and repeat a verified block.
	slices.Reverse(blocks)
	archive.Reset()
	require.NoError(t, car.WriteAll(&archive, header.Roots, append([]car.Block{blocks[0]}, blocks...)))

	loaded, commit, closer, err := LoadFromCAROnDisk(bytes.NewReader(archive.Bytes()), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, closer.Close()) })
	require.NoError(t, commit.VerifySignature(key.PublicKey()))
	require.Equal(t, original.DID, loaded.DID)

	var payloads, want [][]byte
	err = loaded.Tree.Walk(func(_ string, cid cbor.CID) error {
		data, err := loaded.Store.GetBlock(cid)
		if err != nil {
			return err
		}
		expected, err := original.Store.GetBlock(cid)
		if err != nil {
			return err
		}
		payloads = append(payloads, data)
		want = append(want, expected)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, payloads, 256)

	store, ok := closer.(*diskBlockStore)
	require.True(t, ok)
	file := store.file
	info, err := file.Stat()
	require.NoError(t, err)
	var size int64
	for _, block := range blocks {
		size += int64(len(block.Data))
	}
	require.Equal(t, size, info.Size())
	require.NoError(t, closer.Close())
	_, err = file.Stat()
	require.ErrorIs(t, err, os.ErrClosed)
	_, err = os.Stat(file.Name())
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Equal(t, want, payloads)
}

func TestLoadFromCAROnDisk_MissingRoot(t *testing.T) {
	t.Parallel()

	original, key := buildTestRepo(t, 8)
	var archive bytes.Buffer
	require.NoError(t, original.ExportCAR(&archive, key))
	header, blocks, err := car.ReadAll(bytes.NewReader(archive.Bytes()))
	require.NoError(t, err)
	var payloadBytes uint64
	for _, block := range blocks {
		payloadBytes += uint64(len(block.Data))
	}
	budget := NewCARSpillBudget(payloadBytes)
	var missingRoot bytes.Buffer
	require.Equal(t, header.Roots[0], blocks[0].CID)
	require.NoError(t, car.WriteAll(&missingRoot, header.Roots, blocks[1:]))
	_, _, closer, err := LoadFromCAROnDisk(&missingRoot, budget)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.ErrorIs(t, err, mst.ErrBlockNotFound)
	require.Nil(t, closer)
	require.Zero(t, budget.used.Load())

	_, _, closer, err = LoadFromCAROnDisk(&archive, budget)
	require.NoError(t, err)
	require.Equal(t, payloadBytes, budget.used.Load())
	require.NoError(t, closer.Close())
	require.Zero(t, budget.used.Load())
}

func BenchmarkLoadFromCAROnDisk(b *testing.B) {
	for b.Loop() {
		_, _, closer, err := LoadFromCAROnDisk(bytes.NewReader(benchRepoCAR), nil)
		if err != nil {
			b.Fatal(err)
		}
		if err := closer.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
