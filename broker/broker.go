package broker

import (
	"context"
	"sync"
	"time"

	"github.com/numericals/queueSys/storage"
	types "github.com/numericals/queueSys/types"
)

// DeadLetterQueue         []types.Message
// MaxDeliveryAttempt      int
// VisibilityTimeout       int
// DefaultRetryDelay       time.Duration
// Messages                []types.Message

type Broker struct {
	Producers                 []types.Producer
	Consumers                 []types.Consumer
	Queues                    map[string]*Queue
	Notify                    chan bool
	Storage                   storage.Storage
	LastAppliedEventID        uint64
	EventsSinceLastSnapshot   uint64
	SnapshotNotify            chan struct{}
	DefaultVisibilityTimeout  time.Duration
	DefaultRetryDelay         time.Duration
	DefaultMaxAllowedDelay    time.Duration
	DefaultMaxDeliveryAttempt int
	DefaultMessageLimit       int
	Mu                        sync.RWMutex
	Ctx                       context.Context
	Wg                        sync.WaitGroup
}

type Queue struct {
	Name            string
	Messages        []types.Message
	DeadLetterQueue []types.Message
	Config          types.QueueConfig
	Mu              sync.RWMutex
	Metadata        QueueMetadata
}

type QueueMetadata struct {
	CurrentConsumerCount uint64
	TotalPublished       uint64
	TotalConsumed        uint64
}
