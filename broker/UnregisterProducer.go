package broker

import (
	"log"
	"net"
)

func (b *Broker) UnregisterProducer(conn net.Conn) {
	b.Mu.Lock()
	defer b.Mu.Unlock()

	i, err := b.FindProducerIndex(conn)

	if err != nil {
		log.Println(err)
		return
	}

	b.RemoveProducer(i)
}
