package services

import (
	"encoding/json"
	"fmt"
	"log"

	Constants "github.com/numericals/queueSys/constant"
	Types "github.com/numericals/queueSys/types"
	Utils "github.com/numericals/queueSys/utils"
)

func Dispatcher() {
	for {
		available := <-Constants.Notify
		// Constants.Mu.Lock()
		var consumers *[]Types.Consumer = &Constants.Consumer
		var Messages *[]Types.Message = &Constants.Message
		Message := Utils.GetEarliestMessage(*Messages)
		var filteredConsumer *Types.Consumer
		var foundConsumer bool
		filteredConsumer, foundConsumer = Utils.FindConsumer(consumers)

		fmt.Println("we are at dispatcher", filteredConsumer)
		for _, consumer := range *consumers {
			fmt.Println(consumer.Conn.RemoteAddr().String())
		}

		if available && Message != nil && filteredConsumer != nil {

			if !foundConsumer {
				log.Println("Dispatcher: No idle consumers available right now.")
				continue
			}

			payload, err := json.Marshal(Message)
			if err != nil {
				log.Fatalln("unable to marshal the json", err)
				// Constants.Mu.Unlock()
				continue
			}
			fmt.Println("msg go to", filteredConsumer.Conn.RemoteAddr().String())
			_, err = filteredConsumer.Conn.Write(payload)
			if err != nil {
				log.Println("Failed to write to consumer:", err)
				// Constants.Mu.Unlock()
				continue
			}
			Utils.UpdateConsumerStatus(*consumers, Types.BUSY, filteredConsumer.ConsumerId)
			Utils.UpdateMessageProgress(*Messages, Types.PROCESS, Message.MessageId, filteredConsumer.ConsumerId)
			// Constants.Mu.Unlock()
		}
	}
}
