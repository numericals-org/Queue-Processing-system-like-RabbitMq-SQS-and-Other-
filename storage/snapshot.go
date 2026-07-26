package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/numericals/queueSys/types"
	"github.com/numericals/queueSys/utils"
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

func (w *WAL) CreateSnapshot(messages []types.Message, deadLetterQueue []types.Message) error {

	snapshotMessages := ConvertMessagesToSnapshotMessages(messages)
	snapshotDLQ := ConvertMessagesToSnapshotMessages(deadLetterQueue)

	snapshot := Snapshot{
		Metadata: Metadata{
			Version:            1,
			LastAppliedEventID: w.NextEventID - 1,
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

func (w *WAL) CleanUp(LastAppliedEventID uint64) {
	files, err := os.ReadDir(w.walFilePath)

	if err != nil {
		log.Println("issue in reading directory", err)
		return
	}

	files = utils.SortFilesArray(files)

	for _, file := range files {
		deleteTheFile := true
		if w.file.Name() == file.Name() {
			continue
		}

		openfile, err := os.Open(w.walFilePath + file.Name())
		if err != nil {
			log.Println("error in opening file", err)
			continue
		}
		scanner := bufio.NewScanner(openfile)

		for scanner.Scan() {
			var event types.WALEvent

			if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
				fmt.Println("failed to unmarshal replay event: %w", err)
				deleteTheFile = false
				break
			}

			if event.WalId <= LastAppliedEventID {
				continue
			}
			deleteTheFile = false
			break
		}

		if err := scanner.Err(); err != nil {
			fmt.Println("error reading WAL stream: %w", err)
			deleteTheFile = false
		}
		err = openfile.Close()

		if err != nil {
			log.Println("error in closing file", err)
			continue
		}

		if deleteTheFile == true {
			err := os.Remove(w.walFilePath + file.Name())

			if err != nil {
				log.Println("error in remove file", err)
				continue
			}
		}
	}
}
