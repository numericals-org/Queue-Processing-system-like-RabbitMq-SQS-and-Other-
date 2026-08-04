package broker

import (
	"time"

	"github.com/google/uuid"
	"github.com/numericals/queueSys/types"
)

func (b *Broker) CreateMessage(request types.PublishRequest) *types.Message {
	msg := &types.Message{
		MessageId: uuid.New().String(),
		Content:   request.Content,
	}

	if request.TTL > 0 {
		msg.ExpireAt = time.Now().Add(request.TTL)
	}

	if request.Delay > 0 {
		msg.RetrieveAt = time.Now().Add(request.Delay)
		msg.Progress = types.WAITING
	}

	if request.RetryAfter > 0 {
		msg.RetryAfter = request.RetryAfter
	}

	return msg
}
