package broker

import "log"

func (b *Broker) Shutdown(flush func() error) {
	b.CloseAllConnections()

	b.Wg.Wait()
	if err := flush(); err != nil {
		log.Println("failed to flush snapshot:", err)
	}
	if err := b.Storage.Close(); err != nil {
		log.Println(err)
	}
}
