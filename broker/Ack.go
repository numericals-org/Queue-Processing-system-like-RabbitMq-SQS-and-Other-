package broker

import (
	"encoding/json"
	"net"

	"github.com/numericals/queueSys/types"
)

func (b *Broker) Ack(payload json.RawMessage, conn net.Conn) error {

	var request types.AckRequest
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

	if err := b.Commit(types.TASK_ACK, request.MessageId, consumer.ConsumerId, nil, request.QueueName, nil); err != nil {
		Queue.Mu.Unlock()
		return err
	}

	err = Queue.RemoveMessage(index)

	if err != nil {
		Queue.Mu.Unlock()
		return err
	}

	Queue.Metadata.TotalConsumed++
	Queue.Mu.Unlock()

	b.UpdateConsumerStatus(consumer, types.IDLE)
	b.WakeDispatcher()

	return nil
}
