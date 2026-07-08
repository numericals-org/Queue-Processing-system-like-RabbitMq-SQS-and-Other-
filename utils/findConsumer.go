package utils

import (
	Types "github.com/numericals/queueSys/types"
)

func FindConsumer(consumers *[]Types.Consumer) (*Types.Consumer, bool) {

	n := len(*consumers)
	if n <= 0 {
		return nil, false
	}

	for i := range *consumers {
		consumer := (*consumers)[i]
		if consumer.Status == Types.IDLE {
			*consumers = append((*consumers)[:i], (*consumers)[i+1:]...)
			*consumers = append(*consumers, consumer)
			return &(*consumers)[len(*consumers)-1], true
		}
	}

	return nil, false
}
