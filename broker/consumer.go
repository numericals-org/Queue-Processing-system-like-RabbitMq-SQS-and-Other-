package broker

import (
	"fmt"
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

func (b *Broker) FindConsumerByConn(conn net.Conn) *types.Consumer {
	b.Mu.RLock()
	defer b.Mu.RUnlock()
	for i := range b.Consumers {
		consumer := &b.Consumers[i]
		if consumer.Conn == conn {
			return consumer
		}
	}
	return nil
}

func (b *Broker) UpdateConsumerStatusById(status types.Status, consumerId string) error {
	b.Mu.Lock()
	defer b.Mu.Unlock()
	for i := range b.Consumers {
		consumer := &b.Consumers[i]
		if consumer.ConsumerId == consumerId {
			consumer.Status = status
			return nil
		}
	}
	return fmt.Errorf("consumer with id: %s not found", consumerId)
}

func (b *Broker) UpdateConsumerStatus(consumer *types.Consumer, status types.Status) {
	b.Mu.Lock()
	consumer.Status = status
	b.Mu.Unlock()
}

func (b *Broker) FindConsumerIndex(conn net.Conn) (int, error) {
	for i := range b.Consumers {
		if b.Consumers[i].Conn == conn {
			return i, nil
		}
	}

	return 0, fmt.Errorf("Consumer not found")
}

func (b *Broker) RemoveConsumer(index int) {
	b.Consumers = append((b.Consumers)[:index], (b.Consumers)[index+1:]...)
}
