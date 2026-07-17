package broker

import (
	"log"

	"github.com/numericals/queueSys/types"
)

func (b *Broker) Apply(event types.WALEvent) {
	switch event.EventType {

	case types.TASK_QUEUE:
		b.Messages = append(b.Messages, *event.Message)

	case types.TASK_ACK:
		b.RemoveMessage(event.Message.MessageId)

	case types.TASK_DISPATCH:
		MSG := b.FindMessageById(event.MessageId)
		if MSG == nil {
			log.Println("Message not found at Apply in Task_Dispatch")
		}
		b.MarkMessageProcessing(event.MessageId, event.ConsumerId, event.Time)

	case types.TASK_DISAVOW:
		MSG := b.FindMessageById(event.MessageId)
		if MSG == nil {
			log.Println("Message not found at Apply in TASK_DISAVOW")
		}
		b.RequeueMessage(event.MessageId, event.ConsumerId)

	case types.TASK_TIMEOUT:
		MSG := b.FindMessageById(event.MessageId)
		if MSG == nil {
			log.Println("Message not found at Apply in TASK_TIMEOUT")
		}
		b.RequeueMessage(event.MessageId, event.ConsumerId)

	case types.TASK_CONSUMER_DOWN:
		MSG := b.FindMessageById(event.MessageId)
		if MSG == nil {
			log.Println("Message not found at Apply in TASK_TIMEOUT")
		}
		b.RequeueMessage(event.MessageId, event.ConsumerId)
	}
}
