package storage

import (
	"fmt"
	"os"
)

type WAL struct {
	file         *os.File
	snapshotFile *os.File
	NextEventID  uint64
}

func NewWal(path string, snapshotPath string) (*WAL, error) {

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0640)

	if err != nil {
		return nil, fmt.Errorf("failed to open wal file: %w", err)
	}

	snapshotfile, err := os.OpenFile(snapshotPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0640)
	if err != nil {
		return nil, fmt.Errorf("failed to open wal file: %w", err)
	}

	wal := &WAL{
		file:         file,
		snapshotFile: snapshotfile,
	}

	return wal, nil
}
