package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/numericals/queueSys/types"
)

type Metadata struct {
	Version            uint64
	LastAppliedEventID uint64
}

type SnapshotMessage struct {
	MessageId        string
	Content          []byte
	Progress         types.MProgress
	DeliveryAttempts int
	LastConsumerId   string
	RetryAfter       time.Duration
	RetrieveAt       time.Time
}

type Snapshot struct {
	Metadata        Metadata
	Messages        []SnapshotMessage
	DeadLetterQueue []SnapshotMessage
}

func (w *WAL) CreateSnapshot(messages []types.Message, deadLetterQueue []types.Message, lastAppliedEventID uint64) error {

	snapshotMessages := ConvertMessagesToSnapshotMessages(messages)
	snapshotDLQ := ConvertMessagesToSnapshotMessages(deadLetterQueue)

	snapshot := Snapshot{
		Metadata: Metadata{
			Version:            1,
			LastAppliedEventID: lastAppliedEventID,
		},
		DeadLetterQueue: snapshotDLQ,
		Messages:        snapshotMessages,
	}

	snapshotByte, err := json.Marshal(snapshot)

	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}

	file, err := os.OpenFile(w.snapshotTempFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0640)

	if err != nil {
		return fmt.Errorf("failed truncate to file: %w", err)
	}

	_, err = file.Write(snapshotByte)

	if err != nil {
		return fmt.Errorf("failed writing to cache: %w", err)
	}

	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("failed to sync wal file: %w", err)
	}

	if err := file.Close(); err != nil {
		return err
	}

	if err := os.Rename(file.Name(), w.snapshotFile); err != nil {
		return fmt.Errorf("rename snapshot: %w", err)
	}

	return nil
}

func (w *WAL) LoadSnapshot() (*Snapshot, error) {

	data, err := os.ReadFile(w.snapshotFile)

	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read snapshot: %w", err)
	}

	if len(data) <= 0 {
		return nil, nil
	}

	var snapshot Snapshot

	if err := json.Unmarshal(data, &snapshot); err != nil {
		return nil, err
	}

	return &snapshot, nil
}
