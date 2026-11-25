//go:build !windows

package repo

import (
	"errors"
	"fmt"
	"os"
)

func openCARSpillFile() (*os.File, error) {
	file, err := os.CreateTemp("", "atmos-repo-*.blocks")
	if err != nil {
		return nil, err
	}
	if err := unlinkCARSpillFile(file, os.Remove); err != nil {
		return nil, err
	}
	return file, nil
}

func unlinkCARSpillFile(file *os.File, remove func(string) error) error {
	if err := remove(file.Name()); err != nil {
		// Refuse to write payloads to a file that could outlive the process.
		unlinkErr := fmt.Errorf("unlink temporary block store: %w", err)
		closeErr := file.Close()
		return errors.Join(unlinkErr, closeErr, remove(file.Name()))
	}
	return nil
}
