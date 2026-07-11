package broker

import (
	"net"

	types "github.com/numericals/queueSys/types"
)

func (b *Broker) FindConsumer() (*types.Consumer, bool) {
	n := len(b.Consumers)
	if n <= 0 {
		return nil, false
	}
	for i := range b.Consumers {
		consumer := b.Consumers[i]
		if consumer.Status == types.IDLE {
			b.Consumers = append((b.Consumers)[:i], (b.Consumers)[i+1:]...)
			b.Consumers = append(b.Consumers, consumer)
			return &(b.Consumers)[len(b.Consumers)-1], true
		}
	}
	return nil, false
}

func (b *Broker) UpdateConsumerStatus(status types.Status, conn net.Conn) *string {
	for i := range b.Consumers {
		consumer := &b.Consumers[i]
		if consumer.Conn == conn {
			consumer.Status = status
			return &consumer.ConsumerId
		}
	}
	return nil
}
