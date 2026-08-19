package broker

import (
	"fmt"
	"time"

	"github.com/numericals/queueSys/types"
)

func (b *Broker) Commit(task types.WALEType, messageId string, consumerId string, msg *types.Message, QueueName string, QueueConfig *types.QueueConfig) error {
	err := b.Storage.Append(types.WALEvent{
		EventType:   task,
		MessageId:   messageId,
		ConsumerId:  consumerId,
		QueueName:   QueueName,
		Time:        time.Now(),
		Message:     msg,
		QueueConfig: QueueConfig,
	})

	if err != nil {
		return fmt.Errorf("commit unsuccessfully: %w", err)
	}
	b.EventsSinceLastSnapshot++
	select {
	case b.SnapshotNotify <- struct{}{}:
	default:
	}

	return nil
}
