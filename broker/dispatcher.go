package broker

import (
	"encoding/json"
	"fmt"
	"log"

	types "github.com/numericals/queueSys/types"
)

func (b *Broker) Dispatcher() {
	for {
		available := <-b.Notify
		b.Mu.RLock()
		Message := b.GetEarliestMessage()
		b.Mu.RUnlock()

		fmt.Println("Dispatcher", Message)
		fmt.Println("Dispatcher notify", available)

		if available && Message != nil && Message.DeliveryAttempts <= b.MaxDeliveryAttempt {
			filteredConsumer, foundConsumer := b.FindConsumer()
			if !foundConsumer {
				log.Println("Dispatcher: No idle consumers available right now.")
				continue
			}
			payload, err := json.Marshal(Message)
			if err != nil {
				log.Println("unable to marshal the json", err)
				continue
			}
			_, err = filteredConsumer.Conn.Write(payload)
			if err != nil {
				log.Println("Failed to write to consumer:", err)
				continue
			}
			b.UpdateConsumerStatus(types.BUSY, filteredConsumer.Conn)
			b.Mu.Lock()
			b.Commit(types.TASK_DISPATCH, Message.MessageId, filteredConsumer.ConsumerId, nil)
			b.UpdateMessageProgress(types.PROCESS, Message.MessageId, filteredConsumer.ConsumerId)
			b.Mu.Unlock()
		} else if Message != nil {
			b.Mu.Lock()
			b.DeadLetterQueue = append(b.DeadLetterQueue, *Message)
			b.Commit(types.TASK_DEAD_QUEUE, Message.MessageId, Message.ConsumerId, nil)
			b.RemoveMessage(Message.MessageId)
			b.Mu.Unlock()
		}
	}
}
