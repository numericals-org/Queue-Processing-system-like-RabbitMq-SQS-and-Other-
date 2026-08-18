package broker

import (
	"fmt"
	"time"

	"github.com/numericals/queueSys/types"
)

func (b *Broker) RetryWatcher() {
	defer b.Wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-b.Ctx.Done():
			return
		case <-ticker.C:
			hasReadyMessage := false
			b.Mu.RLock()
			queues := make([]*Queue, 0, len(b.Queues))
			for _, q := range b.Queues {
				queues = append(queues, q)
			}
			b.Mu.RUnlock()

			now := time.Now()
			for _, queue := range queues {
				queue.Mu.RLock()
				for i := range queue.Messages {
					msg := &queue.Messages[i]
					if msg.Progress != types.WAITING {
						continue
					}

					if !msg.RetrieveAt.After(now) {
						fmt.Println("got new message in REtry watcher", msg.RetrieveAt)
						hasReadyMessage = true
					}
				}
				queue.Mu.RUnlock()
			}

			if hasReadyMessage == true {
				b.WakeDispatcher()
			}
		}
	}
}
