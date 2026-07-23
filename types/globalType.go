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
	READY MProgress = iota
	WAITING
	PROCESS
	DELETE
)

type Packet struct {
	Type       Mtype
	MessageId  string
	Content    []byte
	RetryAfter time.Duration
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
)

type WALEvent struct {
	WalId      uint64
	EventType  WALEType
	MessageId  string
	ConsumerId string
	Message    *Message
	Time       time.Time
}
