package broker

import (
	"time"

	types "github.com/numericals/queueSys/types"
)

func (b *Broker) GetEarliestMessage() *types.Message {
	for i := range b.Messages {
		msg := &b.Messages[i]

		if msg.Progress != types.WAITING {
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

func (b *Broker) RemoveMessage(consumerId string) {
	var index int

	for i := range b.Messages {
		if b.Messages[i].ConsumerId == consumerId && b.Messages[i].Progress == types.PROCESS {
			index = i
		}
	}

	if len(b.Messages) <= 0 {
		return
	}

	b.Messages = append(b.Messages[:index], b.Messages[index+1:]...)
}

func (b *Broker) RemoveMessageById(messageId string) {
	var index int

	for i := range b.Messages {
		if b.Messages[i].MessageId == messageId {
			index = i
		}
	}

	if len(b.Messages) <= 0 {
		return
	}

	b.Messages = append(b.Messages[:index], b.Messages[index+1:]...)
}

func (b *Broker) RetrieveMessage(consumerId string, duration time.Duration) {
	for i := range b.Messages {
		message := &b.Messages[i]
		if message.ConsumerId == consumerId && message.Progress == types.PROCESS {
			message.Progress = types.WAITING
			message.LastConsumerId = message.ConsumerId
			message.ConsumerId = ""
			message.RetrieveAt = time.Now().Add(duration)
			return
		}
	}
}
