package broker

import (
	"fmt"
	"time"

	"github.com/numericals/queueSys/types"
)

func (b *Broker) Commit(task types.WALEType, messageId string, consumerId string, msg *types.Message, QueueName string) error {
	err := b.Storage.Append(types.WALEvent{
		EventType:  task,
		MessageId:  messageId,
		ConsumerId: consumerId,
		QueueName:  QueueName,
		Time:       time.Now(),
		Message:    msg,
	})

	fmt.Print("Commit", messageId, consumerId)

	if err != nil {
		return fmt.Errorf("commit unsuccessfully", err)
	}
	b.EventsSinceLastSnapshot++
	b.SnapshotNotify <- struct{}{}

	return nil
}
