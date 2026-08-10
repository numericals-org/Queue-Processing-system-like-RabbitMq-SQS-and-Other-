package broker

import (
	"encoding/json"
	"fmt"
	"net"

	"github.com/google/uuid"
	"github.com/numericals/queueSys/types"
)

func (b *Broker) RegisterConsumer(payload json.RawMessage, conn net.Conn) error {

	var request types.RegisterConsumerRequest

	if err := json.Unmarshal(payload, &request); err != nil {
		return err
	}

	b.Mu.Lock()
	defer b.Mu.Unlock()

	for _, name := range request.Queues {
		if _, ok := b.Queues[name]; !ok {
			return fmt.Errorf("Queue doesn't exist", name)
		}
	}

	b.Consumers = append(b.Consumers, types.Consumer{
		Conn:             conn,
		ConsumerId:       uuid.New().String(),
		Status:           types.IDLE,
		SubscribedQueues: request.Queues,
	})

	b.WakeDispatcher()
	return nil
}
