package broker

import (
	"log"

	"github.com/numericals/queueSys/types"
)

func (b *Broker) CloseAllConnections() {
	b.Mu.Lock()
	consumers := append([]types.Consumer(nil), b.Consumers...)
	producers := append([]types.Producer(nil), b.Producers...)
	b.Mu.Unlock()

	for _, conn := range consumers {
		if err := conn.Conn.Close(); err != nil {
			log.Println("failed to close connection:", err)
		}
	}

	for _, conn := range producers {
		if err := conn.Conn.Close(); err != nil {
			log.Println("failed to close connection:", err)
		}
	}
}
