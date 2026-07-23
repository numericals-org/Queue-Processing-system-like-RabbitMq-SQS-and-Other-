package broker

import (
	"sync"
	"time"

	"github.com/numericals/queueSys/storage"
	types "github.com/numericals/queueSys/types"
)

type Broker struct {
	Producers               []types.Producer
	Consumers               []types.Consumer
	Messages                []types.Message
	Notify                  chan bool
	Mu                      sync.RWMutex
	DeadLetterQueue         []types.Message
	MaxDeliveryAttempt      int
	VisibilityTimeout       int
	DefaultRetryDelay       time.Duration
	Storage                 storage.Storage
	LastAppliedEventID      uint64
	EventsSinceLastSnapshot uint64
	SnapshotNotify          chan struct{}
}
