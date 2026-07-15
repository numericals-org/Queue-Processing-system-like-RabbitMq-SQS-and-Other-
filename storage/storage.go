package storage

import (
	"encoding/json"
	"fmt"

	"github.com/numericals/queueSys/types"
)

type Storage interface {
	Append(event types.WALEvent) error
	Replay() ([]types.WALEvent, error)
	Close() error
}

func (w *WAL) Append(event types.WALEvent) error {

	payload, err := json.Marshal(event)

	if err != nil {
		return fmt.Errorf("failed to marshal WAL event: %w", err)
	}

	_, err = w.file.Write(append(payload, '\n'))

	if err != nil {
		return fmt.Errorf("failed writing to cache: %w", err)
	}

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync wal file: %w", err)
	}

	return nil
}

func (w *WAL) Replay() ([]types.WALEvent, error) {
	return nil, nil
}

func (w *WAL) Close() error {
	return w.file.Close()
}
