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