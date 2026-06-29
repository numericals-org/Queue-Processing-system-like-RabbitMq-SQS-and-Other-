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
