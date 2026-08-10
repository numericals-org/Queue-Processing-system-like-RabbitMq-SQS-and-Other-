package broker

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/numericals/queueSys/types"
)

func (b *Broker) Publish(payload json.RawMessage) error {
	var request types.PublishRequest

	if err := json.Unmarshal(payload, &request); err != nil {
		return err
	}

	b.Mu.RLock()
	Queue, ok := b.Queues[request.QueueName]
	if !ok {
		b.Mu.Unlock()
		log.Println("Queue doesn't exists")
		return fmt.Errorf("Queue doesn't exists with name", request.QueueName)
	}
	b.Mu.RUnlock()

	req := request

	Queue.Mu.RLock()
	if request.Delay > 0 {
		if Queue.Config.EnableDelayQueue {
			if request.Delay > Queue.Config.MaxAllowedDelay {
				Queue.Mu.RUnlock()
				return fmt.Errorf("request delay is more than max allowed", Queue.Name)
			}
		} else {
			Queue.Mu.RUnlock()
			return fmt.Errorf("Delay is not allow in queue", Queue.Name)
		}
	}

	defaultRetry := Queue.Config.DefaultRetryDelay
	Queue.Mu.RUnlock()

	if req.RetryAfter == 0 {
		req.RetryAfter = defaultRetry
	}

	msg := b.CreateMessage(req)

	Queue.Mu.Lock()
	defer Queue.Mu.Unlock()

	if Queue.Config.MaxMessages > 0 && len(Queue.Messages) >= Queue.Config.MaxMessages {
		log.Println("Queue is full", Queue.Name)
		return fmt.Errorf("Queue is full", Queue.Name)
	}
	b.Commit(types.TASK_QUEUE, "", "", msg, Queue.Name)
	Queue.Messages = append(Queue.Messages, *msg)
	Queue.Metadata.TotalPublished++

	b.WakeDispatcher()
	return nil
}
