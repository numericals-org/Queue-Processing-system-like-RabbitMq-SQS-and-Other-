package utils

import (
	Types "github.com/numericals/queueSys/types"
)

func GetEarliestMessage(Messages []Types.Message) *Types.Message {
	for i := range Messages {
		if Messages[i].Progress == Types.WAITING {
			return &Messages[i]
		}
	}
	return nil
}

func RemoveMessage(Messages []Types.Message, consumerId string) *[]Types.Message {
	var index int

	for i := range Messages {
		if Messages[i].ConsumerId == consumerId && Messages[i].Progress == Types.PROCESS {
			index = i
		}
	}

	if len(Messages) <= 0 {
		return nil
	}

	Messages = append(Messages[:index], Messages[index+1:]...)

	return &Messages
}
