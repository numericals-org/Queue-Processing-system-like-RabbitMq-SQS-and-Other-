package broker

import (
	"time"

	"github.com/numericals/queueSys/types"
)

func (b *Broker) RecoverInFlightMessages() {
	b.Mu.Lock()
	for i := range b.Messages {
		msg := &b.Messages[i]

		if msg.Progress != types.PROCESS {
			continue
		}

		msg.Progress = types.WAITING
		msg.ConsumerId = ""
		msg.ProcessingStartedAt = time.Time{}
	}
	b.Mu.Unlock()

	b.Notify <- true
}
