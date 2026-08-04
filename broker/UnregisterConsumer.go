package broker

import (
	"log"
	"net"
)

func (b *Broker) UnregisterConsumer(conn net.Conn) {
	b.Mu.Lock()
	defer b.Mu.Unlock()

	i, err := b.FindConsumerIndex(conn)

	if err != nil {
		log.Println(err)
		return
	}

	b.RemoveConsumer(i)
}
