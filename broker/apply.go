package broker

import (
	"fmt"

	"github.com/numericals/queueSys/types"
	"github.com/numericals/queueSys/utils"
)

func (b *Broker) Apply(event types.WALEvent) error {
	switch event.EventType {

	case types.TASK_CREATE_QUEUE:
		if event.QueueConfig == nil {
			return fmt.Errorf("missing QueueConfig for TASK_CREATE_QUEUE")
		}
		if err := b.CreateQueue(event.QueueName, utils.WithRequestConfig(*event.QueueConfig)); err != nil {
			return err
		}
	case types.TASK_DELETE_QUEUE:
		if err := b.DeleteQueue(event.QueueName); err != nil {
			return err
		}
	case types.TASK_QUEUE:
		if event.Message == nil {
			return fmt.Errorf("missing message for TASK_QUEUE")
		}

		queue, err := b.GetQueue(event.QueueName)
		if err != nil {
			return err
		}
		queue.Mu.Lock()
		queue.Messages = append(queue.Messages, *event.Message)
		queue.Metadata.TotalPublished++
		queue.Mu.Unlock()
	case types.TASK_ACK:
		queue, err := b.GetQueue(event.QueueName)
		if err != nil {
			return err
		}
		if err := queue.ApplyAckMessage(event.MessageId); err != nil {
			return fmt.Errorf("failed to apply TASK_ACK: %w", err)
		}
	case types.TASK_DISPATCH:
		queue, err := b.GetQueue(event.QueueName)
		if err != nil {
			return err
		}
		if err := queue.ApplyDispatchMessage(
			event.MessageId,
			event.ConsumerId,
			event.Time,
		); err != nil {
			return fmt.Errorf("failed to apply TASK_DISPATCH: %w", err)
		}
	case types.TASK_DISAVOW:
		queue, err := b.GetQueue(event.QueueName)
		if err != nil {
			return err
		}
		if err := queue.ApplyRequeueMessage(event.MessageId, event.ConsumerId, event.Time); err != nil {
			return err
		}
	case types.TASK_TIMEOUT:
		queue, err := b.GetQueue(event.QueueName)
		if err != nil {
			return err
		}
		if err := queue.ApplyRequeueMessage(event.MessageId, event.ConsumerId, event.Time); err != nil {
			return err
		}
	case types.TASK_CONSUMER_DOWN:
		queue, err := b.GetQueue(event.QueueName)
		if err != nil {
			return err
		}
		if err := queue.ApplyRequeueMessage(event.MessageId, event.ConsumerId, event.Time); err != nil {
			return err
		}
	}

	return nil
}
