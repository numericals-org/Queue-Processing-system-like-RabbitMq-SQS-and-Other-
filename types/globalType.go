package types

import (
	"net"
	"time"
)

type QueueConfig struct {
	MaxDeliveryAttempt    int
	VisibilityTimeout     time.Duration
	DefaultRetryDelay     time.Duration
	EnableDelayQueue      bool
	MaxAllowedDelay       time.Duration
	MaxMessages           int
	EnableDeadLetterQueue bool
}

type QueueOption func(*QueueConfig)

// Type related to Consumers and Producers

type Status int

const (
	IDLE Status = iota
	BUSY
	DOWN
)

func (s Status) String() string {
	return [...]string{"IDLE", "BUSY", "DOWN"}[s]
}

type Producer struct {
	Conn       net.Conn
	ProducerId string
}

type Consumer struct {
	Conn       net.Conn
	ConsumerId string
	Status     Status

	SubscribedQueues []string
}

// Type related Messages

type MProgress int

const (
	READY MProgress = iota
	WAITING
	PROCESS
	DELETE
	DEAD
)

type Message struct {
	MessageId           string
	Content             []byte
	Progress            MProgress
	ConsumerId          string
	DeliveryAttempts    int
	ProcessingStartedAt time.Time
	LastConsumerId      string
	RetryAfter          time.Duration
	RetrieveAt          time.Time
	ExpireAt            time.Time
}

// types related to WAL(write ahead logs)

type WALEType int

const (
	TASK_QUEUE WALEType = iota
	TASK_DISPATCH
	TASK_ACK
	TASK_DISAVOW
	TASK_TIMEOUT
	TASK_CONSUMER_DOWN
	TASK_DEAD_QUEUE
	TASK_RETRY_READY
	TASK_CREATE_QUEUE
	TASK_DELETE_QUEUE
	TASK_EXPIRE
)

type WALEvent struct {
	WalId     uint64
	EventType WALEType

	QueueName   string
	QueueConfig *QueueConfig

	MessageId  string
	ConsumerId string
	Message    *Message
	Time       time.Time
}
