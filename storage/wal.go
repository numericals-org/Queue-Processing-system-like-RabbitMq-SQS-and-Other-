package storage

import (
	"fmt"
	"os"
)

type WAL struct {
	file *os.File
}

func NewWal(path string) (*WAL, error) {

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0640)

	if err != nil {
		return nil, fmt.Errorf("failed to open wal file: %w", err)
	}

	wal := &WAL{
		file: file,
	}

	return wal, nil
}
