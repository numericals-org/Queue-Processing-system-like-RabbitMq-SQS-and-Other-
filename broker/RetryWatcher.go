package broker

import (
	"fmt"
	"time"

	"github.com/numericals/queueSys/types"
)

func (b *Broker) RetryWatcher() {
	for {
		time.Sleep(1 * time.Second)
		retrieved := false
		b.Mu.Lock()

		for i := range b.Messages {
			msg := &b.Messages[i]
			if msg.Progress != types.WAITING {
				continue
			}

			if time.Now().After(msg.RetrieveAt) || time.Now().Equal(msg.RetrieveAt) && len(b.Consumers) > 0 {
				fmt.Println("got new message in REtry watcher", msg.RetrieveAt)
				b.Commit(types.TASK_RETRY_READY, msg.MessageId, msg.ConsumerId, nil)
				msg.RetrieveAt = time.Now().Add(msg.RetryAfter)
				retrieved = true
			}
		}

		b.Mu.Unlock()
		if retrieved == true {
			b.Notify <- true
		}
	}
}
