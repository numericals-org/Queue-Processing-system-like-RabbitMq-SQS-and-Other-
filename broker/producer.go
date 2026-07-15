package broker

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"

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
			b.RetrieveMessage(*consumerId, 0)
			b.Mu.Unlock()
			b.Notify <- true
			return
		}

		var MSG types.Message
		err = json.Unmarshal(buffer[:length], &MSG)
		if err != nil {
			log.Println("unable to Unmarshal the json", err)
		}

		switch MSG.Mtype {
		case types.REGISTER_P:
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
			b.Mu.Unlock()
			b.Notify <- true
		case types.QUEUE:
			b.Mu.Lock()
			err := b.Storage.Append(types.WALEvent{
				EventType: types.TASK_QUEUE,
				Message:   &MSG,
				Time:      time.Now(),
			})
			if err != nil {
				b.Mu.Unlock()
				log.Println("failed to persist message:", err)
				return
			}
			b.Messages = append(b.Messages, MSG)
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
				fmt.Println(MSG, MSG.RetryAfter)
				b.RetrieveMessage(*consumerId, MSG.RetryAfter)
			} else {
				b.RetrieveMessage(*consumerId, b.DefaultRetryDelay)
			}
			b.Mu.Unlock()
			b.Notify <- true
		case types.ACKNOWLEDGE:
			b.Mu.Lock()
			consumerId := b.UpdateConsumerStatus(types.IDLE, Conn)
			if consumerId == nil {
				log.Println("can't get the consumerId", err)
			}
			b.RemoveMessage(*consumerId)
			b.Mu.Unlock()
			b.Notify <- true
		}
	}
}
