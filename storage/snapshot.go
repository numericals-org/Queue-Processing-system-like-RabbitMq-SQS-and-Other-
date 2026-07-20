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

	err = w.snapshotFile.Truncate(0)

	if err != nil {
		return fmt.Errorf("failed truncate to file: %w", err)
	}

	_, err = w.snapshotFile.Write(snapshotByte)

	if err != nil {
		return fmt.Errorf("failed writing to cache: %w", err)
	}

	if err := w.snapshotFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync wal file: %w", err)
	}

	w.snapshotFile.Close()
	return nil
}

func (w *WAL) LoadSnapshot() (*Snapshot, error) {

	data, err := os.ReadFile(w.snapshotFile.Name())

	if err != nil {
		return nil, err
	}

	if len(data) <= 0 {
		return nil, nil
	}

	var snapshot Snapshot

	if err := json.Unmarshal(data, &snapshot); err != nil {
		fmt.Println(data, err)
		return nil, err
	}

	fmt.Println(snapshot)

	return &snapshot, nil
}
