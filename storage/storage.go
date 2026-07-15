package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"

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
	file, err := os.Open(w.file.Name())
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL for replay: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	var estimatedCount int

	if err == nil && stat.Size() > 0 {
		estimatedCount = int(stat.Size() / 150)
	}

	events := make([]types.WALEvent, 0, estimatedCount)

	scanner := bufio.NewScanner(file)

	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 512*1024)

	for scanner.Scan() {
		var event types.WALEvent

		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return nil, fmt.Errorf("failed to unmarshal replay event: %w", err)
		}

		events = append(events, event)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading WAL stream: %w", err)
	}

	return events, nil
}

func (w *WAL) Close() error {
	return w.file.Close()
}
