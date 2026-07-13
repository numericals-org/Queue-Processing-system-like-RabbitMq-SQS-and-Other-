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

type Message struct {
	MessageId           string
	Content             []byte
	Mtype               Mtype
	Progress            MProgress
	ConsumerId          string
	DeliveryAttempts    int
	ProcessingStartedAt time.Time
	LastConsumerId      string
	RetryAfter          time.Duration
	RetrieveAt          time.Time
}
