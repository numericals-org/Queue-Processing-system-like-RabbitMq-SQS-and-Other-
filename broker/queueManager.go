package broker

import (
	"fmt"

	"github.com/numericals/queueSys/types"
	"github.com/numericals/queueSys/utils"
)

func (b *Broker) CreateQueue(name string, userConfig ...types.QueueOption) error {

	verify := utils.TestName(name, `^[a-z0-9-]+$`)
	if !verify {
		return fmt.Errorf("Invalid queue name")
	}

	config := types.QueueConfig{
		MaxDeliveryAttempt: b.DefaultMaxDeliveryAttempt,
		VisibilityTimeout:  b.DefaultVisibilityTimeout,
		DefaultRetryDelay:  b.DefaultRetryDelay,
		MaxMessages:        b.DefaultMessageLimit,
		MaxAllowedDelay:    b.DefaultMaxAllowedDelay,
	}

	for _, opt := range userConfig {
		opt(&config)
	}

	err := ValidateConfig(config)

	if err != nil {
		return err
	}

	b.Mu.Lock()
	defer b.Mu.Unlock()
	_, ok := b.Queues[name]
	if ok {
		return fmt.Errorf("Queue already exists")
	}

	b.Queues[name] = &Queue{
		Name:   name,
		Config: config,
	}

	return nil
}

func (b *Broker) DeleteQueue(name string) error {
	b.Mu.Lock()
	defer b.Mu.Unlock()

	_, ok := b.Queues[name]

	if !ok || len(b.Queues[name].Messages) > 0 {
		return fmt.Errorf("Not able to delete this queue")
	}

	delete(b.Queues, name)
	return nil
}

func (b *Broker) GetQueue(name string) (*Queue, error) {
	b.Mu.RLock()
	defer b.Mu.RUnlock()
	queue, ok := b.Queues[name]
	if !ok {
		return nil, fmt.Errorf("Queue does not exist")
	}

	return queue, nil
}

func (b *Broker) ListQueues() []*Queue {
	b.Mu.RLock()
	defer b.Mu.RUnlock()
	queues := make([]*Queue, 0, len(b.Queues))

	for _, value := range b.Queues {
		queues = append(queues, value)
	}

	return queues
}
