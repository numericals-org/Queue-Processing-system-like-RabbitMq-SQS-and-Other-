package types

import (
	"net"
	"time"
)

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
}

// Type related Messages

type Mtype int

const (
	QUEUE Mtype = iota
	REGISTER_P
	REGISTER_C
	ACKNOWLEDGE
	DISAVOW
	C_STATUS
)

type MProgress int

const (
	WAITING MProgress = iota
	PROCESS
	DELETE
)

type Packet struct {
	Type       Mtype         `json:"type"`
	MessageId  string        `json:"messageId,omitempty"`
	Content    []byte        `json:"content,omitempty"`
	RetryAfter time.Duration `json:"retryAfter,omitempty"`
}

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
}

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
)

type WALEvent struct {
	EventType  WALEType
	MessageId  string
	ConsumerId string
	Message    *Message
	Time       time.Time
}
