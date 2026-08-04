package broker

import "github.com/numericals/queueSys/types"

func (b *Broker) Nack(request *types.NackRequest, consumer *types.Consumer) error {
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

	b.Commit(types.TASK_DISAVOW, request.MessageId, consumer.ConsumerId, nil, request.QueueName)

	// Reserved for future adaptive retry support.
	// Currently ignored by the broker.
	// msg := &Queue.Messages[index]

	// if request.RetryAfter > 0 {
	// 	msg.RetryAfter = request.RetryAfter
	// }

	Queue.RequeueMessage(index)

	return nil
}
