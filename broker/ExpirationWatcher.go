package broker

import (
	"log"
	"time"

	"github.com/numericals/queueSys/types"
)

func (b *Broker) ExpirationWatcher() {
	defer b.Wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-b.Ctx.Done():
			return
		case <-ticker.C:
			b.Mu.RLock()
			queues := make([]*Queue, 0, len(b.Queues))
			for _, q := range b.Queues {
				queues = append(queues, q)
			}
			b.Mu.RUnlock()

			now := time.Now()
			for _, queue := range queues {
				queue.Mu.Lock()
				for i := range queue.Messages {
					msg := &queue.Messages[i]
					if msg.Progress != types.WAITING && msg.Progress != types.READY {
						continue
					}

					if !msg.ExpireAt.After(now) {
						if err := b.Commit(types.TASK_EXPIRE, msg.MessageId, msg.ConsumerId, nil, queue.Name, nil); err != nil {
							log.Println(err)
							continue
						}
						if err := queue.RemoveMessage(i); err != nil {
							log.Println(err)
							continue
						}
					}
				}
				queue.Mu.Unlock()
			}
		}
	}
}
