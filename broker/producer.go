package broker

import (
	"encoding/json"
	"fmt"
	"log"
	"net"

	"github.com/google/uuid"
	"github.com/numericals/queueSys/types"
)

func (b *Broker) Receiver(Conn net.Conn) {
	buffer := make([]byte, 1024)

	for {
		length, err := Conn.Read(buffer)
		if err != nil {
			log.Println("Can't read Message from Connection", err)
			b.Mu.Lock()
			consumerId := b.UpdateConsumerStatus(types.DOWN, Conn)
			if consumerId == nil {
				b.Mu.Unlock()
				return
			}
			b.RetrieveMessages(*consumerId, b.DefaultRetryDelay, types.TASK_CONSUMER_DOWN)
			b.Mu.Unlock()
			b.Notify <- true
			return
		}
		var MSG types.Packet
		err = json.Unmarshal(buffer[:length], &MSG)
		if err != nil {
			log.Println("unable to Unmarshal the json", err)
		}

		fmt.Println("line before switch", MSG)
		fmt.Printf("MSG.Type = %v (%T)\n", MSG.Type, MSG.Type)
		fmt.Printf("REGISTER_P = %v (%T)\n", types.REGISTER_P, types.REGISTER_P)
		fmt.Println(MSG.Type == types.REGISTER_P)
		switch MSG.Type {
		case types.REGISTER_P:
			fmt.Println("Register_P", MSG)
			b.Mu.Lock()
			b.Producers = append(b.Producers, types.Producer{
				Conn:       Conn,
				ProducerId: uuid.New().String(),
			})
			b.Mu.Unlock()
		case types.REGISTER_C:
			b.Mu.Lock()
			ID := uuid.New().String()
			b.Consumers = append(b.Consumers, types.Consumer{
				Conn:       Conn,
				ConsumerId: ID,
				Status:     types.IDLE,
			})
			fmt.Println("Register_C", MSG)
			b.Mu.Unlock()
			b.Notify <- true
		case types.QUEUE:
			b.Mu.Lock()
			log.Println("MESSAGE OBJECT AT PRODUCER- Line no 61", MSG)
			Message := b.CreateMessage(MSG.Content, MSG.RetryAfter)
			b.Commit(types.TASK_QUEUE, "", "", Message)
			b.Messages = append(b.Messages, *Message)
			fmt.Println("QUEUE", MSG)
			b.Mu.Unlock()
			b.Notify <- true
		case types.DISAVOW:
			b.Mu.Lock()
			consumerId := b.UpdateConsumerStatus(types.IDLE, Conn)
			if consumerId == nil {
				b.Mu.Unlock()
				return
			}
			if MSG.RetryAfter != 0 {
				b.Commit(types.TASK_DISAVOW, MSG.MessageId, *consumerId, nil)
				b.RetrieveMessage(MSG.MessageId, *consumerId, MSG.RetryAfter, types.TASK_DISAVOW)
			} else {
				b.Commit(types.TASK_DISAVOW, MSG.MessageId, *consumerId, nil)
				b.RetrieveMessage(MSG.MessageId, *consumerId, b.DefaultRetryDelay, types.TASK_DISAVOW)
			}
			fmt.Println("DISAVOW", MSG)
			b.Mu.Unlock()
			b.Notify <- true
		case types.ACKNOWLEDGE:
			b.Mu.Lock()
			consumerId := b.UpdateConsumerStatus(types.IDLE, Conn)
			if consumerId == nil {
				log.Println("can't get the consumerId", err)
			}
			b.Commit(types.TASK_ACK, MSG.MessageId, *consumerId, nil)
			b.RemoveMessage(MSG.MessageId)
			fmt.Println("ACKNOWLEDGE", MSG)
			b.Mu.Unlock()
			b.Notify <- true
		}
	}
}
