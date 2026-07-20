package broker

import (
	"time"

	"github.com/google/uuid"
	"github.com/numericals/queueSys/types"
)

func (b *Broker) CreateMessage(content []byte, RetryAfter time.Duration) *types.Message {
	return &types.Message{
		MessageId:  uuid.New().String(),
		Content:    content,
		Progress:   types.READY,
		RetryAfter: RetryAfter,
	}
}
