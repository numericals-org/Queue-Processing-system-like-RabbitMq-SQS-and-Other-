package broker

import (
	"time"

	types "github.com/numericals/queueSys/types"
)

func (b *Broker) GetEarliestMessage() *types.Message {
	for i := range b.Messages {
		msg := &b.Messages[i]

		if msg.Progress != types.WAITING && msg.Progress != types.READY {
			continue
		}

		if time.Now().Before(msg.RetrieveAt) {
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

func (b *Broker) RemoveMessage(messageId string) {
	var index int

	for i := range b.Messages {
		if b.Messages[i].MessageId == messageId {
			index = i
			break
		}
	}

	if len(b.Messages) <= 0 {
		return
	}

	b.Messages = append(b.Messages[:index], b.Messages[index+1:]...)
}

func (b *Broker) RetrieveMessages(consumerId string, duration time.Duration, task types.WALEType) {
	for i := range b.Messages {
		message := &b.Messages[i]
		if message.ConsumerId == consumerId && message.Progress == types.PROCESS {
			message.Progress = types.WAITING
			message.LastConsumerId = message.ConsumerId
			message.ConsumerId = ""
			message.RetrieveAt = time.Now().Add(duration)
			message.RetryAfter = duration

			return
		}
	}
}

func (b *Broker) RetrieveMessage(MessageId string, consumerId string, duration time.Duration, task types.WALEType) {
	for i := range b.Messages {
		message := &b.Messages[i]
		if message.MessageId == MessageId {
			b.Storage.Append(types.WALEvent{
				EventType:  task,
				ConsumerId: consumerId,
				MessageId:  message.MessageId,
				Time:       time.Now(),
			})
			message.Progress = types.WAITING
			message.LastConsumerId = message.ConsumerId
			message.ConsumerId = ""
			message.RetrieveAt = time.Now().Add(duration)
			message.RetryAfter = duration

			return
		}
	}
}

func (b *Broker) FindMessageById(messageId string) *types.Message {

	for i := range b.Messages {
		if b.Messages[i].MessageId == messageId {
			return &b.Messages[i]
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
