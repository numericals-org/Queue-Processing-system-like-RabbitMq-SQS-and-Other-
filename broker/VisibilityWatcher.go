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
			retrieved := false
			b.Mu.Lock()

			for i := range b.Messages {
				msg := &b.Messages[i]
				if msg.Progress != types.PROCESS {
					continue
				}

				timeout := time.Since(msg.ProcessingStartedAt)

				if timeout >= time.Duration(b.VisibilityTimeout)*time.Second {
					fmt.Println("got new message in visibitlity watcher", msg.RetrieveAt)
					b.Commit(types.TASK_TIMEOUT, msg.MessageId, msg.ConsumerId, nil)
					b.RetrieveMessage(msg.MessageId, msg.ConsumerId, msg.RetryAfter, types.TASK_TIMEOUT)
					retrieved = true
				}
			}

			b.Mu.Unlock()
			if retrieved == true {
				b.Notify <- true
			}
		}
	}
}
