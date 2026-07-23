package storage

import (
	"fmt"
	"os"
	"strconv"
)

type WAL struct {
	walFilePath      string
	file             *os.File
	snapshotFile     string
	snapshotTempFile string
	NextEventID      uint64
	WalId            uint64
}

func NewWal(path string, snapshotPath string, snapshotTempPath string) (*WAL, error) {

	filePath := path + "wal_" + strconv.FormatUint(0, 10)
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0640)

	if err != nil {
		return nil, fmt.Errorf("failed to open wal file: %w", err)
	}

	wal := &WAL{
		walFilePath:      path,
		file:             file,
		snapshotFile:     snapshotPath,
		snapshotTempFile: snapshotTempPath,
	}
	return wal, nil
}
