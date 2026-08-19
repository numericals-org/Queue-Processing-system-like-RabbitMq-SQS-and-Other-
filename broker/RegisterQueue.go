package broker

import (
	"encoding/json"

	"github.com/numericals/queueSys/types"
	"github.com/numericals/queueSys/utils"
)

func (b *Broker) RegisterQueue(payload json.RawMessage) error {
	var request types.CreateQueueRequest
	if err := json.Unmarshal(payload, &request); err != nil {
		return err
	}

	if err := b.Commit(types.TASK_CREATE_QUEUE, "", "", nil, request.QueueName, &request.QueueConfig); err != nil {
		return err
	}

	if err := b.CreateQueue(request.QueueName, utils.WithRequestConfig(request.QueueConfig)); err != nil {
		return err
	}

	return nil
}
