package broker

import (
	"fmt"
	"net"

	"github.com/numericals/queueSys/types"
)

func (b *Broker) FindProducerIndex(conn net.Conn) (int, error) {
	for i, p := range b.Producers {
		if p.Conn == conn {
			return i, nil
		}
	}

	return 0, fmt.Errorf("Producer not found")
}

func (b *Broker) FindProducerByConn(conn net.Conn) *types.Producer {
	b.Mu.RLock()
	defer b.Mu.RUnlock()
	for i, p := range b.Producers {
		if p.Conn == conn {
			return p
		}
	}

	return nil
}

func (b *Broker) RemoveProducer(index int) {
	b.Producers = append(b.Producers[:index], b.Producers[index+1:]...)
}
