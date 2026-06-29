package services

import (
	"encoding/json"
	"log"

	Constants "github.com/numericals/queueSys/constant"
	Types "github.com/numericals/queueSys/types"
	Utils "github.com/numericals/queueSys/utils"
)

func Dispatcher() {
	for {
		available := <-Constants.Notify
		var consumers *[]Types.Consumer = &Constants.Consumer
		var Messages *[]Types.Message = &Constants.Message
		Message := Utils.GetEarliestMessage(*Messages)
		var filteredConsumer Types.Consumer
		foundConsumer := false

		if available && Message != nil {
			for _, consumer := range *consumers {
				if consumer.Status == Types.IDLE {
					filteredConsumer = consumer
					foundConsumer = true
					break
				}
			}

			if !foundConsumer {
				log.Println("Dispatcher: No idle consumers available right now.")
				continue
			}

			payload, err := json.Marshal(Message)
			if err != nil {
				log.Fatalln("unable to marshal the json", err)
				continue
			}
			_, err = filteredConsumer.Conn.Write(payload)
			if err != nil {
				log.Println("Failed to write to consumer:", err)
				continue
			}
			Utils.UpdateValueInArray(*consumers, Types.BUSY, "Status", filteredConsumer.ConsumerId)
			Utils.UpdateValueInArray(*Messages, Types.PROCESS, "Progress", Message.MessageId)
		}
	}
}
