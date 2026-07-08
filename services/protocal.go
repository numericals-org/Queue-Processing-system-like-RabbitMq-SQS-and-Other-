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
			*producers = append(*producers, Types.Producer{
				Conn:       Conn,
				ProducerId: uuid.New().String(),
			})
		case Types.REGISTER_C:
			ID := uuid.New().String()
			*consumers = append(*consumers, Types.Consumer{
				Conn:       Conn,
				ConsumerId: ID,
				Status:     Types.IDLE,
			})
			Constants.Notify <- true
		case Types.QUEUE:
			*Queue = append(*Queue, MSG)
			Constants.Notify <- true
		case Types.ACKNOWLEDGE:
			for _, val := range *consumers {
				if Conn == val.Conn {
					Utils.UpdateConsumerStatus(*consumers, Types.IDLE, val.ConsumerId)
					newQueue := Utils.RemoveMessage(*Queue, val.ConsumerId)
					if newQueue != nil {
						Queue = newQueue
					}
				}
			}
			Constants.Notify <- true
		}
	}
}
