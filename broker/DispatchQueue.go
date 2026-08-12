package broker

import (
	"encoding/json"
	"log"

	"github.com/numericals/queueSys/types"
)

func (b *Broker) DispatchQueue(queue *Queue) {

	queue.Mu.Lock()
	i, err := queue.FindReadyMessage()

	if err != nil {
		queue.Mu.Unlock()
		log.Println("no ready message")
		return
	}

	messageId := queue.Messages[i].MessageId
	queue.Mu.Unlock()

	consumer := b.ReserveIdleConsumer(queue.Name)
	if consumer == nil {
		log.Println("no consumer find")
		return
	}

	queue.Mu.Lock()

	i, err = queue.FindMessageIndex(messageId)

	if err != nil {
		queue.Mu.Unlock()
		b.UpdateConsumerStatus(consumer, types.IDLE)
		log.Println("no ready message")
		return
	}

	message := queue.Messages[i]

	if message.Progress != types.WAITING && message.Progress != types.READY {
		queue.Mu.Unlock()
		b.UpdateConsumerStatus(consumer, types.IDLE)
		log.Println("Message picked by other dispatcher")
		return
	}

	b.Commit(types.TASK_DISPATCH, message.MessageId, consumer.ConsumerId, nil, queue.Name)
	queue.DispatchMessage(i, consumer.ConsumerId)
	queue.Mu.Unlock()

	payload, err := json.Marshal(message)
	if err != nil {
		b.UpdateConsumerStatus(consumer, types.IDLE)
		log.Println("unable to marshal the json", err)
		return
	}

	_, err = consumer.Conn.Write(payload)
	if err != nil {
		b.HandleDisconnect(consumer.Conn)
		log.Println("Failed to write to consumer:", err)
		return
	}

}
