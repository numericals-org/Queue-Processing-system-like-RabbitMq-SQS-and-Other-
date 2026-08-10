package broker

import (
	"net"

	"github.com/google/uuid"
	"github.com/numericals/queueSys/types"
)

func (b *Broker) RegisterProducer(conn net.Conn) {
	b.Mu.Lock()
	defer b.Mu.Unlock()

	b.Producers = append(b.Producers, types.Producer{
		Conn:       conn,
		ProducerId: uuid.New().String(),
	})

	b.WakeDispatcher()
	return
}
