package broker

import (
	types "github.com/numericals/queueSys/types"
)

func (b *Broker) GetEarliestMessage() *types.Message {
	for i := range b.Messages {
		if b.Messages[i].Progress == types.WAITING {
			return &b.Messages[i]
		}
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

func (b *Broker) RetrieveMessage(consumerId string) {
	for i := range b.Messages {
		message := &b.Messages[i]
		if message.ConsumerId == consumerId && message.Progress == types.PROCESS {
			message.Progress = types.WAITING
			message.ConsumerId = ""
			return
		}
	}
}
