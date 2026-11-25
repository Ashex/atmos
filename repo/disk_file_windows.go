package repo

import (
	"crypto/rand"
	"os"
	"path/filepath"

	"golang.org/x/sys/windows"
)

func openCARSpillFile() (*os.File, error) {
	path := filepath.Join(os.TempDir(), "atmos-repo-"+rand.Text()+".blocks")
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL|windows.FILE_FLAG_DELETE_ON_CLOSE, 0o600)
}
