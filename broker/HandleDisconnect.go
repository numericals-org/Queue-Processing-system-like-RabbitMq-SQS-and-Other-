package broker

import (
	"net"

	"github.com/numericals/queueSys/types"
)

func (b *Broker) HandleDisconnect(conn net.Conn) {
	consumer := b.FindConsumerByConn(conn)

	if consumer != nil {
		b.UpdateConsumerStatus(consumer, types.DOWN)
		b.RequeueConsumerMessages(consumer.ConsumerId)
		b.UnregisterConsumer(conn)
		b.WakeDispatcher()
		return
	}

	producer := b.FindProducerByConn(conn)

	if producer != nil {
		b.UnregisterProducer(conn)
		return
	}

	return
}
