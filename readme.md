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

### DAY 3 - Finish Broker Version 0.1
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

### DAY 4 - Fixing Data Races issue
In the start i just Mutex randomly any where after testing code i git some issue so i add on place where its actually needed

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