package types

import (
	"encoding/json"
	"time"
)

type Mtype int

const (
	REGISTER_P Mtype = iota
	REGISTER_C
	CREATE_QUEUE
	DELETE_QUEUE
	PUBLISH
	ACK
	NACK
)

type Packet struct {
	Type    Mtype           `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type CreateQueueRequest struct {
	QueueName   string
	QueueConfig QueueConfig
}

type PublishRequest struct {
	QueueName  string
	Content    []byte
	RetryAfter time.Duration
	Delay      time.Duration
	TTL        time.Duration
}

type AckRequest struct {
	QueueName string
	MessageId string
}

type NackRequest struct {
	QueueName  string
	MessageId  string
	RetryAfter time.Duration
}

type DeleteQueueRequest struct {
	QueueName string
}

type RegisterConsumerRequest struct {
	Queues []string
}
