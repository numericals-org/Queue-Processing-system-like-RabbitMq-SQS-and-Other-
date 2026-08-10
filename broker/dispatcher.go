package broker

func (b *Broker) Dispatcher() {
	defer b.Wg.Done()
	for {
		select {
		case <-b.Notify:
			queueList := b.ListQueues()
			for i := range queueList {
				b.DispatchQueue(queueList[i])
			}
		case <-b.Ctx.Done():
			return
		}
	}
}
