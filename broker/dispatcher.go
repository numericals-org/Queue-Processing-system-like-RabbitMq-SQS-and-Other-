package broker

import (
	"encoding/json"
	"log"

	types "github.com/numericals/queueSys/types"
)

func (b *Broker) Dispatcher() {
	for {
		available := <-b.Notify
		b.Mu.RLock()
		Message := b.GetEarliestMessage()
		b.Mu.RUnlock()

		if available && Message != nil {
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
			b.UpdateMessageProgress(types.PROCESS, Message.MessageId, filteredConsumer.ConsumerId)
		}
	}
}
