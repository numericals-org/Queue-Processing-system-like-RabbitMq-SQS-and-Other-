package broker

import (
	"fmt"
	"time"

	"github.com/numericals/queueSys/types"
)

func (b *Broker) VisibilityWatcher() {
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

			expiredConsumerIds := []string{}

			for _, queue := range queues {
				queue.Mu.Lock()
				for i := range queue.Messages {
					msg := &queue.Messages[i]

					if msg.Progress != types.PROCESS {
						continue
					}

					timeout := time.Since(msg.ProcessingStartedAt)

					if timeout >= time.Duration(queue.Config.VisibilityTimeout)*time.Second {
						consumerId := msg.ConsumerId
						fmt.Println("got new message in visibitlity watcher", msg.RetrieveAt)
						b.Commit(types.TASK_TIMEOUT, msg.MessageId, msg.ConsumerId, nil, queue.Name)
						queue.RetrieveMessage(msg)
						expiredConsumerIds = append(expiredConsumerIds, consumerId)
					}
				}
				queue.Mu.Unlock()
			}

			for _, consumerId := range expiredConsumerIds {
				b.UpdateConsumerStatusById(consumerId, types.IDLE)
			}
			if len(expiredConsumerIds) > 0 {
				b.WakeDispatcher()
			}
		}
	}
}
