package broker

import (
	"encoding/json"
	"net"

	"github.com/numericals/queueSys/types"
)

func (b *Broker) Nack(payload json.RawMessage, conn net.Conn) error {

	var request types.NackRequest

	if err := json.Unmarshal(payload, &request); err != nil {
		return err
	}

	consumer := b.FindConsumerByConn(conn)

	Queue, err := b.GetQueue(request.QueueName)
	if err != nil {
		return err
	}

	Queue.Mu.Lock()
	index, err := Queue.FindMessageIndex(request.MessageId)

	if err != nil {
		Queue.Mu.Unlock()
		return err
	}

	if err := b.Commit(types.TASK_DISAVOW, request.MessageId, consumer.ConsumerId, nil, request.QueueName, nil); err != nil {
		Queue.Mu.Unlock()
		return err
	}

	// Reserved for future adaptive retry support.
	// Currently ignored by the broker.
	// msg := &Queue.Messages[index]

	// if request.RetryAfter > 0 {
	// 	msg.RetryAfter = request.RetryAfter
	// }

	Queue.RequeueMessage(index)
	Queue.Mu.Unlock()

	b.UpdateConsumerStatus(consumer, types.IDLE)

	b.WakeDispatcher()

	return nil
}
