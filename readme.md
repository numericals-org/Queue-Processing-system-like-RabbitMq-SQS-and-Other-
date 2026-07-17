# Queue Processing system (like RabbitMq, SQS, and Other)

Today we start with an architecture where a Broker with 3 service
- Message Queue
- Protocols
- Dispatcher

## our journey for today
### DAY 1 - Protocol
we build a tcp in our main.go we use standard library of go which is net. the PORT we use is 6464.
we Declare are some of type which look like this right now

#### Consumer&Producer Types

```
status
enum "IDLE" or "BUSY" or "DOWN" 
Tell the status of connections
```

```
Producer
{
    Conn: "which is actual connection of producers who send message to our broker"
    ProducerId: string; "Unique Identifier"
}
```

```
Consumer
{
    Conn: "which is actual connection of Consumer who receive message to our broker"
    ConsumerId: string; "Unique Identifier"
    Status: enum; "status Enum which we create before"
}
```

#### Messages Type
```
Mtype
enum  QUEUE or REGISTER_P or REGISTER_C or ACKNOWLEDGE  or C_STATUS

to understand the Message Purpose
```

```
Message
{
    MessageId: string; "Unique Identifier"
    Content: buffer; "using Buffer for get any type or kind of value come to server"
    Mtype: enum: "Mtype Enum which we create before"
}
```

#### Folder structure
```
root
|_____ constant (folder for constant and global variables)
|
|_____ services (folder where all services store like:- protocol, dispatcher)
|
|_____ types (folder where all types and enum files is stored)
|
|_main.go (entry point)
```

### DAY 2 - Changes and fix dispatcher
we complete flow where producers share data and consumer get the data but the issue is right now when a new connection connect then we only send message to consumer, consumer state is not updating, and last problem is all message delivered to single consumer which is not a queue behavior its a broadcasting behavior.

#### dispatcher
we add new folder new utils where we write our filter and update algo so latest folder structure look like
```
root
|
|_____ constant (folder for constant and global variables)
|
|_____ services (folder where all services store like:- protocol, dispatcher)
|
|_____ types (folder where all types and enum files is stored)
|
|_____ utils (folder where our all algorithm is written)
|
|_ main.go (entry point)
```

### DAY 3 - Introducing Broker as the Single Source of Truth
we changes global generic function to multiple functions which have single purpose to do like (UpdateConsumerStatus,UpdateMessageProgress). add round robin algorithm for send 1 message at a time to 1 consumer then pick different one and finish life cycle of message

#### add new value in message type
we add a new key which is consumer id so we can identify which message we need to delete from queue.
```
type Message struct {
	MessageId  string
	Content    []byte
	Mtype      Mtype
	Progress   MProgress
	ConsumerId string
}
```

#### round robin algorithm
we fix the round robin architecture. new also is this
```
n := len(*consumers)
	if n <= 0 {
		return nil, false
	}

	for i := range *consumers {
		consumer := (*consumers)[i]
		if consumer.Status == Types.IDLE {
			*consumers = append((*consumers)[:i], (*consumers)[i+1:]...)
			*consumers = append(*consumers, consumer)
			return &(*consumers)[len(*consumers)-1], true
		}
	}

	return nil, false

```

day end with data races issue

### DAY 4 - Eliminating Data Races and Introducing Thread-Safe Broker State
In the start i just Mutex randomly any where after testing code i got some issue so i add on place where its actually needed

so now some file look like this
```
func UpdateMessageProgress(messages []Types.Message, progress Types.MProgress, id string, consumerId string) {
	Constants.Mu.Lock()
	for i := range messages {
		message := &messages[i]
		if message.MessageId == id {
			message.Progress = progress
			message.ConsumerId = consumerId
		}
	}
	Constants.Mu.Unlock()
}
```

```
package services

import (
	"encoding/json"
	"log"
	"net"

	"github.com/google/uuid"
	Constants "github.com/numericals/queueSys/constant"
	Types "github.com/numericals/queueSys/types"
	Utils "github.com/numericals/queueSys/utils"
)

func Receiver(Conn net.Conn) {
	buffer := make([]byte, 1024)
	var producers *[]Types.Producer = &Constants.Producers
	var consumers *[]Types.Consumer = &Constants.Consumer
	var Queue *[]Types.Message = &Constants.Message

	for {
		length, err := Conn.Read(buffer)
		if err != nil {
			log.Fatalln("Can't read Message from Connection", err)
		}
		var MSG Types.Message
		err = json.Unmarshal(buffer[:length], &MSG)
		if err != nil {
			log.Fatalln("unable to Unmarshal the json", err)
		}

		switch MSG.Mtype {
		case Types.REGISTER_P:
			Constants.Mu.Lock()
			*producers = append(*producers, Types.Producer{
				Conn:       Conn,
				ProducerId: uuid.New().String(),
			})
			Constants.Mu.Unlock()
		case Types.REGISTER_C:
			Constants.Mu.Lock()
			ID := uuid.New().String()
			*consumers = append(*consumers, Types.Consumer{
				Conn:       Conn,
				ConsumerId: ID,
				Status:     Types.IDLE,
			})
			Constants.Mu.Unlock()
			Constants.Notify <- true
		case Types.QUEUE:
			Constants.Mu.Lock()
			*Queue = append(*Queue, MSG)
			Constants.Mu.Unlock()
			Constants.Notify <- true
		case Types.ACKNOWLEDGE:
			Constants.Mu.Lock()
			for _, val := range *consumers {
				if Conn == val.Conn {
					Utils.UpdateConsumerStatus(*consumers, Types.IDLE, val.ConsumerId)
					newQueue := Utils.RemoveMessage(*Queue, val.ConsumerId)
					if newQueue != nil {
						Queue = newQueue
					}
				}
			}
			Constants.Mu.Unlock()
			Constants.Notify <- true
		}
	}
}

```

round robin algorithm issue is not just a data race issue it's also a understanding issue which is related to understand when we need to find a new consumer. also when an acknowledgement comes, new message arrive and new consumer joins the connection or when <b> we have to dispatch something.</b>

### DAY 5 - Continue with refactor
make one single struct who own every thing producers, consumers, message, channels, mutex and methods every possible aspect which is present in our QUEUE SYSTEM

after all refactor current Folder Structure is :-
```
root
|
|___ broker
|    |_ broker.go (main broker struct file)
|    |_ brokerMethods.go (file where all broker methods exists)
|
|___ cmd
|    |_ client
|	 |	|_ consumer.go (dummy consumer create script)
|	 |
|	 |_ server
|	 	|_ main.go (dummy producer create script)
|
|___ types
|	 |_ globalType.go (file where all types exists)
|
|_ go.mod
|_ go.sum
|_ main.go
|_ readme.go
```

final folder structure in the end of day 5 is :-
```
root
|
|___ broker
|    |_ broker.go 
|    |_ dispatcher.go
|    |_ consumer.go
|    |_ producer.go
|    |_ queue.go
|
|___ cmd
|    |_ client
|	 |	|_ consumer.go (dummy consumer create script)
|	 |
|	 |_ server
|	 	|_ main.go (dummy producer create script)
|
|___ types
|	 |_ globalType.go (file where all types exists)
|
|_ go.mod
|_ go.sum
|_ main.go
|_ readme.go
```

### DAY 6 - Introducing consumer down & retry mechanism
in this version our broker we add two important feature which help to make message flow more safe and more reliable in real use case

#### Consumer DOWN detection
in current state of broken when consumer network lost is lost so whole broker is go down which cause us message lost issue
so what the plan? the plan is we add one more status for consumer which help to understand consumer is down and after short time we can remove that for our consumer queue and if consumer is up again so we update the status so we get the consumer is ready to start again 

we implement that when the broker detects a read error on a consumer connection so update status like this
```
if err != nil {
			log.Println("Can't read Message from Connection", err)
			b.Mu.Lock()
			b.UpdateConsumerStatus(types.DOWN, Conn)
			b.Mu.Unlock()
			return
		}
```
and we also update UpdateConsumerStatus function because we implement two for loops which unnecessary at the time of acknowledgement comes to producer

#### Message Retrieve Method
we add one more method which is retrieve message method. its use when Consumer DOWN detection detect any consumer is gone down so if there any message is in progress so retrieve them and send them back in queue
code:- 
```
func (b *Broker) RetrieveMessage(consumerId string) {
	for i := range b.Messages {
		message := b.Messages[i]
		if message.ConsumerId == consumerId && message.Progress == types.PROCESS {
			message.Progress = types.WAITING
			message.ConsumerId = ""
		}
	}
}

```
```
if err != nil {
			log.Println("Can't read Message from Connection", err)
			b.Mu.Lock()
			consumerId := b.UpdateConsumerStatus(types.DOWN, Conn)
			b.RetrieveMessage(*consumerId)
			b.Mu.Unlock()
			b.Notify <- true
			return
		}
```

### DAY 7 - Implement NACK and the retry lifecycle.
today we implement NACK and retry lifecycle where we check is consumer process is failed or some other is occurred in consumer so consumer send DISAVOW(NACK) which work as no acknowledgement for broker and if no acknowledgement so we add retry lifecycle for process that message but no as many time as possible there is a limit. right now the limit is globally static for ever message which is 3 so if consumer failed to send acknowledgement for message 3 times we send message in a different queue.

now one more type add in M.type
```
type Mtype int

const (
	QUEUE Mtype = iota
	REGISTER_P
	REGISTER_C
	ACKNOWLEDGE
	DISAVOW
	C_STATUS
)
```
now message look like this
```
type Message struct {
	MessageId  string
	Content    []byte
	DeliveryAttempts int
	Mtype      Mtype
	Progress   MProgress
	ConsumerId string
}
```

for remove message from queue after all deliveryAttempt is done we add a new function which is look like this:- 
```
func (b *Broker) RemoveMessageById(messageId string) {
	var index int

	for i := range b.Messages {
		if b.Messages[i].MessageId == messageId {
			index = i
		}
	}

	if len(b.Messages) <= 0 {
		return
	}

	b.Messages = append(b.Messages[:index], b.Messages[index+1:]...)
}
```
in this function we need to pass message id to remove it from queue. and DeliveryAttempts increase when dispatcher dispatch message for process so no matter what is the reason for message is not delivered its increase the count and max 4 time we try to send them.

and we add a new protocol in receiver which take care of DISAVOW
```
case types.DISAVOW:
			b.Mu.Lock()
			consumerId := b.UpdateConsumerStatus(types.IDLE, Conn)
			if consumerId == nil {
				b.Mu.Unlock()
				return
			}
			b.RetrieveMessage(*consumerId)
			b.Mu.Unlock()
			b.Notify <- true
```
we also update UpdateMessageProgress when this function runs now the the delivery attempt gonna increase:-
```
func (b *Broker) UpdateMessageProgress(progress types.MProgress, id string, consumerId string) {
	// b.Mu.Lock()
	for i := range b.Messages {
		message := &b.Messages[i]
		if message.MessageId == id {
			message.Progress = progress
			message.ConsumerId = consumerId
			message.DeliveryAttempts++
			return
		}
	}
	// b.Mu.Unlock()
}
``` 
who owns the retry magical number is broker so we a one more field in Broker which is MaxDeliveryAttempt and this use every where it needs

### DAY 8 - Visibility Timeout & Retry Delay
we have recover message when consumer disconnect. we have retry message when consumer send NACK for message, but what when consumer never disconnect, send NACK or ACK. our broker wait for confirmation for forever which is wrong so now we add visibility timeout feature. so our broker wait for a particular time if we don't get any response in return so we retrieve the message and notify the broker. so dispatcher can dispatch the message again.

current updated Broker
```
type Broker struct {
	Producers          []types.Producer
	Consumers          []types.Consumer
	Messages           []types.Message
	Notify             chan bool
	Mu                 sync.RWMutex
	DeadLetterQueue    []types.Message
	MaxDeliveryAttempt int
	VisibilityTimeout  int
}
```

we add one more key in messages 
```
type Message struct {
	MessageId           string
	Content             []byte
	Mtype               Mtype
	Progress            MProgress
	ConsumerId          string
	DeliveryAttempts    int
	ProcessingStartedAt time.Time
}
```

we add new service which is VisibilityWatcher. this service is responsible for check every single message in queue which is in process state and timeout is not expire if timeout is expire so retrieve the message and notify the dispatcher.

```
func (b *Broker) VisibilityWatcher() {
	for {
		time.Sleep(1 * time.Second)
		b.Mu.Lock()

		for i := range b.Messages {
			msg := &b.Messages[i]
			if msg.Progress != types.PROCESS {
				continue
			}

			timeout := time.Since(msg.ProcessingStartedAt)

			if timeout >= time.Duration(b.VisibilityTimeout) {
				b.RetrieveMessage(msg.ConsumerId)
				b.Notify <- true
			}
		}

		b.Mu.Unlock()
	}
}
```

current folder structure look like this:- 
```
root
|
|___ broker
|    |_ broker.go 
|    |_ dispatcher.go
|    |_ consumer.go
|    |_ producer.go
|    |_ queue.go
|    |_ VisibilityWatcher.go
|
|___ cmd
|    |_ client
|	 |	|_ consumer.go (dummy consumer create script)
|	 |
|	 |_ server
|	 	|_ main.go (dummy producer create script)
|
|___ types
|	 |_ globalType.go (file where all types exists)
|
|_ go.mod
|_ go.sum
|_ main.go
|_ readme.go
```

face a data race issue at time UpdateMessageProgress call so we add mu.lock at dispatcher and unload after process done

#### Retry Delay
now implement retry delay is save our cpu usage from send unnecessary message dispatch. we implement two pattern here
1. use a global retry delay
2. consumer send us retry delay

consumer sended retry delay overwrite the global retry delay.

we add new field in message now
```
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
```

update retrieve message function:-
```
func (b *Broker) RetrieveMessage(consumerId string, duration time.Duration) {
	for i := range b.Messages {
		message := &b.Messages[i]
		if message.ConsumerId == consumerId && message.Progress == types.PROCESS {
			message.Progress = types.WAITING
			message.LastConsumerId = message.ConsumerId
			message.ConsumerId = ""
			message.RetrieveAt = time.Now().Add(duration)
			return
		}
	}
}
```

update GetEarliestMessage function also:-
```
func (b *Broker) GetEarliestMessage() *types.Message {
	for i := range b.Messages {
		msg := &b.Messages[i]

		if msg.Progress != types.WAITING {
			continue
		}

		if time.Now().Before(msg.RetrieveAt) {
			continue
		}

		return msg
	}
	return nil
}
```

introducing a new service which RetryWatcher. which check every possible message in queue which is possible to resend
```
func (b *Broker) RetryWatcher() {
	for {
		time.Sleep(1 * time.Second)
		retrieved := false
		b.Mu.Lock()

		for i := range b.Messages {
			msg := &b.Messages[i]
			if msg.Progress != types.WAITING {
				continue
			}

			if time.Now().After(msg.RetrieveAt) || time.Now().Equal(msg.RetrieveAt) {
				retrieved = true
			}
		}

		b.Mu.Unlock()
		if retrieved == true {
			b.Notify <- true
		}
	}
}
```

Broker Current flow
```
                     Producer
                         │
                         ▼
                     Receiver
                         │
                         ▼
                    WAITING
                         │
             RetrieveAt expired?
                         │
                         ▼
                    Dispatcher
                         │
                         ▼
                     PROCESS
                    /       \
                  ACK      DISAVOW
                   │           │
                   ▼           ▼
                DELETE    RetrieveMessage
                               │
                               ▼
                            WAITING
                               │
                               ▼
                         RetryWatcher

Consumer Disconnect
        │
        ▼
VisibilityWatcher
        │
        ▼
RetrieveMessage
```

### DAY 9 - A broker should not lose messages when it crashes.
we start design a database for our broker where we recover data when broker is crashed.
current folder structure look like :-
```
root
|
|___ broker
|    |_ broker.go 
|    |_ apply.go 
|    |_ dispatcher.go
|    |_ consumer.go
|    |_ producer.go
|    |_ queue.go
|    |_ VisibilityWatcher.go
|    |_ RetryWatcher.go
|    |_ message.go
|    |_ commit.go
|
|___ cmd
|    |_ client
|	 |	|_ consumer.go (dummy consumer create script)
|	 |
|	 |_ server
|	 	|_ main.go (dummy producer create script)
|
|___ storage
|    |_ storage.go
|    |_ wal.go
|
|___ data
|    |_ wal.log (for store logs)
|
|___ types
|	 |_ globalType.go (file where all types exists)
|
|_ go.mod
|_ go.sum
|_ main.go
|_ readme.md
```

changes the protocol architecture now we Packet type which tell us the Packet type and content look like this:-
```
type Packet struct {
	Type       Mtype         `json:"type"`
	MessageId  string        `json:"messageId,omitempty"`
	Content    []byte        `json:"content,omitempty"`
	RetryAfter time.Duration `json:"retryAfter,omitempty"`
}
```

we also update the message type which use at place of Packets. now message :-
```
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
```

we create a WAL (write ahead logs) system in it which we use for reply the queue when broker restart. there is 3 file belong to WAL.
1. storage.go :-
```
package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"

	"github.com/numericals/queueSys/types"
)

type Storage interface {
	Append(event types.WALEvent) error
	Replay() ([]types.WALEvent, error)
	Close() error
}

func (w *WAL) Append(event types.WALEvent) error {

	payload, err := json.Marshal(event)

	if err != nil {
		return fmt.Errorf("failed to marshal WAL event: %w", err)
	}

	_, err = w.file.Write(append(payload, '\n'))

	if err != nil {
		return fmt.Errorf("failed writing to cache: %w", err)
	}

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync wal file: %w", err)
	}

	return nil
}

func (w *WAL) Replay() ([]types.WALEvent, error) {
	file, err := os.Open(w.file.Name())
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL for replay: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	var estimatedCount int

	if err == nil && stat.Size() > 0 {
		estimatedCount = int(stat.Size() / 150)
	}

	events := make([]types.WALEvent, 0, estimatedCount)

	scanner := bufio.NewScanner(file)

	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 512*1024)

	for scanner.Scan() {
		var event types.WALEvent

		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return nil, fmt.Errorf("failed to unmarshal replay event: %w", err)
		}

		events = append(events, event)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading WAL stream: %w", err)
	}

	return events, nil
}

func (w *WAL) Close() error {
	return w.file.Close()
}

```

in this file we create a interface which use with wal struct. all method belongs tp wal is present in this file like (append, reply, close).

2. wal.go :-
```
package storage

import (
	"fmt"
	"os"
)

type WAL struct {
	file *os.File
}

func NewWal(path string) (*WAL, error) {

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0640)

	if err != nil {
		return nil, fmt.Errorf("failed to open wal file: %w", err)
	}

	wal := &WAL{
		file: file,
	}

	return wal, nil
}

```

in this file we create our main struct for wal and newWal method for create a new log file.

3. wal.log :- in this file all event and msg which receive, dispatch, retrieve, delete, or send Dead Queue record here.

we also create some broker methods a well like (createMessage, commit).
commit.go :-
```
package broker

import (
	"log"
	"time"

	"github.com/numericals/queueSys/types"
)

func (b *Broker) Commit(task types.WALEType, messageId string, consumerId string, msg *types.Message) {
	err := b.Storage.Append(types.WALEvent{
		EventType:  task,
		MessageId:  messageId,
		ConsumerId: consumerId,
		Time:       time.Now(),
		Message:    msg,
	})

	if err != nil {
		log.Println("commit unsuccessfully", err)
	}
}

```
we create this file for manage our single source of task in every function this function only works is append code in WAL

message.go :- 
```
package broker

import (
	"time"

	"github.com/google/uuid"
	"github.com/numericals/queueSys/types"
)

func (b *Broker) CreateMessage(content []byte, RetryAfter time.Duration) *types.Message {
	return &types.Message{
		MessageId: uuid.New().String(),
		Content:   content,
		Progress:  types.WAITING,
	}
}

```
we manage all message which create to send. create at one place

we use them at dispatcher, producers, retryWatcher, and VisibilityWatcher. some implementation:-
```
case types.QUEUE:
			b.Mu.Lock()
			Message := b.CreateMessage(MSG.Content, MSG.RetryAfter)
			b.Commit(types.TASK_QUEUE, "", "", Message)
			b.Messages = append(b.Messages, *Message)
			b.Mu.Unlock()
			b.Notify <- true
```

```
func (b *Broker) RetryWatcher() {
	for {
		time.Sleep(1 * time.Second)
		retrieved := false
		b.Mu.Lock()

		for i := range b.Messages {
			msg := &b.Messages[i]
			if msg.Progress != types.WAITING {
				continue
			}

			if time.Now().After(msg.RetrieveAt) || time.Now().Equal(msg.RetrieveAt) {
				b.Commit(types.TASK_TIMEOUT, msg.MessageId, msg.ConsumerId, nil)
				retrieved = true
			}
		}

		b.Mu.Unlock()
		if retrieved == true {
			b.Notify <- true
		}
	}
}
```

```
case types.ACKNOWLEDGE:
			b.Mu.Lock()
			consumerId := b.UpdateConsumerStatus(types.IDLE, Conn)
			if consumerId == nil {
				log.Println("can't get the consumerId", err)
			}
			b.Commit(types.TASK_ACK, MSG.MessageId, *consumerId, nil)
			b.RemoveMessage(MSG.MessageId)
			b.Mu.Unlock()
			b.Notify <- true
```

we also add some method for Requeue the Message without append logs in wal.log file like RequeueMessage, MarkMessageProcessing
```
func (b *Broker) RequeueMessage(messageId string, consumerId string) {
	for i := range b.Messages {
		message := &b.Messages[i]
		if message.MessageId == messageId {
			message.Progress = types.WAITING
			message.LastConsumerId = consumerId
			message.ConsumerId = ""
		}
	}
}
```

```
func (b *Broker) MarkMessageProcessing(messageId string, consumerId string, ProcessingStartedAt time.Time) {
	for i := range b.Messages {
		message := &b.Messages[i]
		if message.MessageId == messageId {
			message.Progress = types.PROCESS
			message.ConsumerId = consumerId
			message.ProcessingStartedAt = ProcessingStartedAt
		}
	}
}
```