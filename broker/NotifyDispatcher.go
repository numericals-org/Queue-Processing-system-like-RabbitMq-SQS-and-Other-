package broker

func (b *Broker) WakeDispatcher() {
	select {
	case b.Notify <- true:
	case <-b.Ctx.Done():
	}
}
