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
