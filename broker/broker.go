package broker

import (
	"sync"
	"time"

	types "github.com/numericals/queueSys/types"
)

type Broker struct {
	Producers          []types.Producer
	Consumers          []types.Consumer
	Messages           []types.Message
	Notify             chan bool
	Mu                 sync.RWMutex
	DeadLetterQueue    []types.Message
	MaxDeliveryAttempt int
	VisibilityTimeout  int
	DefaultRetryDelay  time.Duration
}
