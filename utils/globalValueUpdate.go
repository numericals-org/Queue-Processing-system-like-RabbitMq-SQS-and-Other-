package utils

import (
	"reflect"

	Types "github.com/numericals/queueSys/types"
)

type ArrayT interface {
	Types.Consumer | Types.Message | Types.Producer
}

type value interface {
	int | string | bool | Types.Mtype | Types.Status | Types.MProgress
}

func UpdateValueInArray[T ArrayT, V value](s []T, value V, key string, id string) {
	for i := range s {
		valReflect := reflect.ValueOf(&s[i]).Elem()

		var currentId string
		switch valReflect.Type().Name() {
		case "Message":
			currentId = valReflect.FieldByName("MessageId").String()
		case "Consumer":
			currentId = valReflect.FieldByName("ConsumerId").String()
		}

		if currentId == id {
			fieldToUpdate := valReflect.FieldByName(key)

			if fieldToUpdate.IsValid() && fieldToUpdate.CanSet() {
				newValReflect := reflect.ValueOf(value)

				if fieldToUpdate.Type() == newValReflect.Type() {
					fieldToUpdate.Set(newValReflect)
					return
				}
			}
		}
	}
}

func UpdateConsumerStatus(consumers []Types.Consumer, status Types.Status, id string) {
	for i := range consumers {
		consumer := &consumers[i]
		if consumer.ConsumerId == id {
			consumer.Status = status
		}
	}
}

func UpdateMessageProgress(messages []Types.Message, progress Types.MProgress, id string, consumerId string) {
	for i := range messages {
		message := &messages[i]
		if message.MessageId == id {
			message.Progress = progress
			message.ConsumerId = consumerId
		}
	}
}
