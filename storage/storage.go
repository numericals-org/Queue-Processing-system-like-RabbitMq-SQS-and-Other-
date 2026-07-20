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
	Replay(LastAppliedEventID uint64) ([]types.WALEvent, uint64, error)
	Close() error
}

func (w *WAL) Append(event types.WALEvent) error {

	event.WalId = w.NextEventID
	w.NextEventID++
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

func (w *WAL) Replay(LastAppliedEventID uint64) ([]types.WALEvent, uint64, error) {
	file, err := os.Open(w.file.Name())
	var highestNumberId uint64

	if err != nil {
		return nil, highestNumberId, fmt.Errorf("failed to open WAL for replay: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	var estimatedCount int

	if err == nil && stat.Size() > 0 {
		// Rough estimate to reduce slice reallocations.
		estimatedCount = int(stat.Size() / 150)
	}

	events := make([]types.WALEvent, 0, estimatedCount)

	scanner := bufio.NewScanner(file)

	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 512*1024)

	for scanner.Scan() {
		var event types.WALEvent

		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return nil, highestNumberId, fmt.Errorf("failed to unmarshal replay event: %w", err)
		}

		if event.WalId > highestNumberId {
			highestNumberId = event.WalId
		}

		if event.WalId <= LastAppliedEventID {
			continue
		}

		events = append(events, event)
	}

	if err := scanner.Err(); err != nil {
		return nil, highestNumberId, fmt.Errorf("error reading WAL stream: %w", err)
	}

	return events, highestNumberId, nil
}

func (w *WAL) Close() error {
	return w.file.Close()
}
