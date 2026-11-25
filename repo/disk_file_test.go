//go:build !windows

package repo

import (
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnlinkCARSpillFile_FailsClosed(t *testing.T) {
	t.Parallel()

	file, err := os.CreateTemp(t.TempDir(), "spill")
	require.NoError(t, err)
	unlinkErr := errors.New("controlled initial unlink failure")
	var attempts int
	err = unlinkCARSpillFile(file, func(path string) error {
		attempts++
		if attempts == 1 {
			return unlinkErr
		}
		_, err := file.Stat()
		require.ErrorIs(t, err, os.ErrClosed)
		return os.Remove(path)
	})
	require.ErrorIs(t, err, unlinkErr)
	require.Equal(t, 2, attempts)
	_, err = os.Stat(file.Name())
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestUnlinkCARSpillFile_ReportsPersistentFailure(t *testing.T) {
	t.Parallel()

	file, err := os.CreateTemp(t.TempDir(), "spill")
	require.NoError(t, err)
	unlinkErr := errors.New("controlled initial unlink failure")
	cleanupErr := errors.New("controlled cleanup failure")
	var attempts int
	err = unlinkCARSpillFile(file, func(_ string) error {
		attempts++
		if attempts == 1 {
			return unlinkErr
		}
		return cleanupErr
	})
	require.ErrorIs(t, err, unlinkErr)
	require.ErrorIs(t, err, cleanupErr)
	_, err = file.Stat()
	require.ErrorIs(t, err, os.ErrClosed)
	info, err := os.Stat(file.Name())
	require.NoError(t, err)
	require.Zero(t, info.Size())
}
