package broker

import (
	"github.com/numericals/queueSys/types"
)

func (b *Broker) Ack(request *types.AckRequest, consumer *types.Consumer) error {
	Queue, err := b.GetQueue(request.QueueName)
	if err != nil {
		return err
	}

	Queue.Mu.Lock()
	defer Queue.Mu.Unlock()

	index, err := Queue.FindMessageIndex(request.MessageId)

	if err != nil {
		return err
	}

	b.Commit(types.TASK_ACK, request.MessageId, consumer.ConsumerId, nil, request.QueueName)
	err = Queue.RemoveMessage(index)

	if err != nil {
		return err
	}

	Queue.Metadata.TotalConsumed++
	return nil
}
