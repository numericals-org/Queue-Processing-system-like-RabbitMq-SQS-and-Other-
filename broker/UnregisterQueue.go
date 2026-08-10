package broker

import (
	"encoding/json"

	"github.com/numericals/queueSys/types"
)

func (b *Broker) UnregisterQueue(payload json.RawMessage) error {
	var request types.CreateQueueRequest
	if err := json.Unmarshal(payload, &request); err != nil {
		return err
	}

	if err := b.DeleteQueue(request.QueueName); err != nil {
		return err
	}

	return nil
}
