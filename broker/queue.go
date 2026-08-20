package broker

import (
	"fmt"
	"log"
	"time"

	types "github.com/numericals/queueSys/types"
)

func (q *Queue) FindReadyMessage() (int, error) {
	now := time.Now()
	for i := range q.Messages {
		message := &q.Messages[i]

		if !message.ExpireAt.IsZero() && !message.ExpireAt.After(now) {
			message.Progress = types.DELETE
			continue
		}

		if message.DeliveryAttempts >= q.Config.MaxDeliveryAttempt {
			message.Progress = types.DEAD
			continue
		}

		if message.Progress == types.READY {
			return i, nil
		}

		if message.Progress == types.WAITING &&
			!message.RetrieveAt.After(now) {
			return i, nil
		}

	}

	return -1, fmt.Errorf("no ready message")
}

// func (b *Broker) MarkMessageProcessing(messageId string, consumerId string, ProcessingStartedAt time.Time) {
// 	for i := range b.Messages {
// 		message := &b.Messages[i]
// 		if message.MessageId == messageId {
// 			message.Progress = types.PROCESS
// 			message.ConsumerId = consumerId
// 			message.ProcessingStartedAt = ProcessingStartedAt
// 			return
// 		}
// 	}
// }

func (q *Queue) DispatchMessage(index int, consumerId string) {
	message := &q.Messages[index]

	message.ConsumerId = consumerId
	message.Progress = types.PROCESS
	message.ProcessingStartedAt = time.Now()
	message.DeliveryAttempts++
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
				if err := b.Commit(types.TASK_CONSUMER_DOWN, msg.MessageId, consumerId, nil, queue.Name, nil); err != nil {
					queue.Mu.Unlock()
					log.Println(err)
					continue
				}
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

func (q *Queue) RetrieveMessage(msg *types.Message) {
	msg.LastConsumerId = msg.ConsumerId
	msg.ConsumerId = ""
	msg.RetrieveAt = time.Now().Add(msg.RetryAfter)
	msg.Progress = types.WAITING
}

func (q *Queue) ApplyDispatchMessage(messageId string, consumerId string, processingStartedAt time.Time) error {
	q.Mu.Lock()
	defer q.Mu.Unlock()

	index, err := q.FindMessageIndex(messageId)
	if err != nil {
		return err
	}

	message := &q.Messages[index]

	message.ConsumerId = consumerId
	message.Progress = types.PROCESS
	message.ProcessingStartedAt = processingStartedAt
	message.DeliveryAttempts++

	return nil
}

func (q *Queue) ApplyAckMessage(messageId string) error {
	q.Mu.Lock()
	defer q.Mu.Unlock()

	index, err := q.FindMessageIndex(messageId)
	if err != nil {
		return err
	}

	if err := q.RemoveMessage(index); err != nil {
		return err
	}

	return nil
}

func (q *Queue) ApplyRequeueMessage(messageId string, consumerId string, eventTime time.Time) error {
	q.Mu.Lock()
	defer q.Mu.Unlock()

	msg := q.FindMessageById(messageId)
	if msg == nil {
		return fmt.Errorf("message %s not found", messageId)
	}

	msg.LastConsumerId = consumerId
	msg.ConsumerId = ""
	msg.RetrieveAt = eventTime.Add(msg.RetryAfter)
	msg.Progress = types.WAITING

	return nil
}

func (q *Queue) ApplyDeadLetterMessage(messageId string) error {
	q.Mu.Lock()
	defer q.Mu.Unlock()

	index, err := q.FindMessageIndex(messageId)
	if err != nil {
		return err
	}

	message := q.Messages[index]

	q.DeadLetterQueue = append(q.DeadLetterQueue, message)
	q.Messages = append(q.Messages[:index], q.Messages[index+1:]...)

	return nil
}
