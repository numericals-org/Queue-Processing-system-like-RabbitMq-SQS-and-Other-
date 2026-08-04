package broker

import (
	"fmt"
	"time"

	types "github.com/numericals/queueSys/types"
)

func (b *Broker) GetEarliestMessage() *types.Message {
	for i := range b.Messages {
		msg := &b.Messages[i]

		if msg.Progress != types.WAITING && msg.Progress != types.READY {
			fmt.Println("type is wrong", msg)
			continue
		}

		if time.Now().Before(msg.RetrieveAt) {
			fmt.Println("Time", msg.RetrieveAt)
			fmt.Println("come before time", msg)
			continue
		}

		return msg
	}
	return nil
}

func (b *Broker) UpdateMessageProgress(progress types.MProgress, id string, consumerId string) {
	// b.Mu.Lock()
	for i := range b.Messages {
		message := &b.Messages[i]
		if message.MessageId == id {
			message.Progress = progress
			message.ConsumerId = consumerId
			message.DeliveryAttempts++
			message.ProcessingStartedAt = time.Now()
			return
		}
	}
	// b.Mu.Unlock()
}

func (q *Queue) RemoveMessage(messageIndex int) error {
	q.Messages = append(q.Messages[:messageIndex], q.Messages[messageIndex+1:]...)
	return nil
}

func (b *Broker) RequeueConsumerMessages(consumerId string) {

	b.Mu.RLock()
	queues := make([]*Queue, 0, len(b.Queues))
	for _, q := range b.Queues {
		queues = append(queues, q)
	}
	b.Mu.RUnlock()

	for _, queue := range queues {
		queue.Mu.Lock()
		for i := range queue.Messages {
			msg := &queue.Messages[i]

			if msg.ConsumerId == consumerId &&
				msg.Progress == types.PROCESS {

				queue.RequeueMessage(i)
			}
		}
		queue.Mu.Unlock()
	}
}

func (q *Queue) FindMessageById(messageId string) *types.Message {
	for i := range q.Messages {
		if q.Messages[i].MessageId == messageId {
			return &q.Messages[i]
		}
	}

	return nil
}

func (b *Broker) RequeueMessage(messageId string, consumerId string) {
	for i := range b.Messages {
		message := &b.Messages[i]
		if message.MessageId == messageId {
			message.Progress = types.WAITING
			message.LastConsumerId = consumerId
			message.ConsumerId = ""

			return
		}
	}
}

func (b *Broker) MarkMessageProcessing(messageId string, consumerId string, ProcessingStartedAt time.Time) {
	for i := range b.Messages {
		message := &b.Messages[i]
		if message.MessageId == messageId {
			message.Progress = types.PROCESS
			message.ConsumerId = consumerId
			message.ProcessingStartedAt = ProcessingStartedAt

			return
		}
	}
}

func (q *Queue) FindMessageIndex(messageId string) (int, error) {
	for i := range q.Messages {
		if q.Messages[i].MessageId == messageId {
			return i, nil
		}
	}

	return 0, fmt.Errorf("message %s not found", messageId)
}

func (q *Queue) RequeueMessage(MessageIndex int) {
	message := &q.Messages[MessageIndex]

	message.LastConsumerId = message.ConsumerId
	message.ConsumerId = ""
	message.RetrieveAt = time.Now().Add(message.RetryAfter)
	message.Progress = types.WAITING
}
