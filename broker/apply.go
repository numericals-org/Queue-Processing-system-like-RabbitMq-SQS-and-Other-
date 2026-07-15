package broker

import "github.com/numericals/queueSys/types"

func (b *Broker) Apply(event types.WALEvent) {
	switch event.EventType {

	case types.TASK_QUEUE:
		b.Messages = append(b.Messages, *event.Message)

	case types.TASK_ACK:
		b.RemoveMessageById(event.Message.MessageId)

	case types.TASK_DISPATCH:
		b.Messages = append(b.Messages, *event.Message)

	case types.TASK_DISAVOW:
		b.Messages = append(b.Messages, *event.Message)
	}
}
