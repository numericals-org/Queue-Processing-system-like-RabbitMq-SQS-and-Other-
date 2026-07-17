package broker

import (
	"time"

	"github.com/numericals/queueSys/types"
)

func (b *Broker) VisibilityWatcher() {
	for {
		time.Sleep(1 * time.Second)
		retrieved := false
		b.Mu.Lock()

		for i := range b.Messages {
			msg := &b.Messages[i]
			if msg.Progress != types.PROCESS {
				continue
			}

			timeout := time.Since(msg.ProcessingStartedAt)

			if timeout >= time.Duration(b.VisibilityTimeout)*time.Second {
				b.Commit(types.TASK_TIMEOUT, msg.MessageId, msg.ConsumerId, nil)
				b.RetrieveMessage(msg.MessageId, msg.ConsumerId, 0, types.TASK_TIMEOUT)
				retrieved = true
			}
		}

		b.Mu.Unlock()
		if retrieved == true {
			b.Notify <- true
		}
	}
}
