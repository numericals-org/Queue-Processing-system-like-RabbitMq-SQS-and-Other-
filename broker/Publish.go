package broker

import (
	"log"

	"github.com/numericals/queueSys/types"
)

func (b *Broker) Publish(request *types.PublishRequest) {
	b.Mu.RLock()
	Queue, ok := b.Queues[request.QueueName]
	if !ok {
		b.Mu.Unlock()
		log.Println("Queue doesn't exists")
		return
	}
	b.Mu.RUnlock()

	req := *request

	Queue.Mu.RLock()
	if request.Delay > 0 {
		if Queue.Config.EnableDelayQueue {
			if request.Delay > Queue.Config.MaxAllowedDelay {
				Queue.Mu.RUnlock()
				log.Println("request delay is more than max allowed", Queue.Name)
				return
			}
		} else {
			Queue.Mu.RUnlock()
			log.Println("Delay is not allow in queue", Queue.Name)
			return
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
		return
	}
	b.Commit(types.TASK_QUEUE, "", "", msg, Queue.Name)
	Queue.Messages = append(Queue.Messages, *msg)
	Queue.Metadata.TotalPublished++

	select {
	case b.Notify <- true:
	case <-b.Ctx.Done():
		return
	}
}
