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

	if err := b.Commit(types.TASK_DELETE_QUEUE, "", "", nil, request.QueueName, &request.QueueConfig); err != nil {
		return err
	}

	if err := b.DeleteQueue(request.QueueName); err != nil {
		return err
	}

	return nil
}
