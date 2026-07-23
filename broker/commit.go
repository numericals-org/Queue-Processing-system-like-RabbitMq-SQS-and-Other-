package broker

import (
	"fmt"
	"log"
	"time"

	"github.com/numericals/queueSys/types"
)

func (b *Broker) Commit(task types.WALEType, messageId string, consumerId string, msg *types.Message) {
	err := b.Storage.Append(types.WALEvent{
		EventType:  task,
		MessageId:  messageId,
		ConsumerId: consumerId,
		Time:       time.Now(),
		Message:    msg,
	})

	fmt.Print("Commit", messageId, consumerId)

	if err != nil {
		log.Println("commit unsuccessfully", err)
		return
	}
	b.EventsSinceLastSnapshot++
	b.SnapshotNotify <- struct{}{}
}
