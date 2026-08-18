package broker

import "github.com/numericals/queueSys/types"

func (b *Broker) RecoverInFlightMessages() {
	b.Mu.RLock()
	queues := make([]*Queue, 0, len(b.Queues))
	for _, q := range b.Queues {
		queues = append(queues, q)
	}
	b.Mu.RUnlock()

	for _, queue := range queues {
		queue.Mu.Lock()
		for i := range queue.Messages {
			msg := &queue.Messages[i]
			if msg.Progress != types.PROCESS {
				continue
			}
			queue.RetrieveMessage(msg)
		}
		queue.Mu.Unlock()
	}
}
