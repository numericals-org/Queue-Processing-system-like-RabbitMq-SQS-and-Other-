package utils

import (
	// Constants "github.com/numericals/queueSys/constant"
	"fmt"

	Types "github.com/numericals/queueSys/types"
)

func FindConsumer(consumers *[]Types.Consumer) (*Types.Consumer, bool) {

	// Constants.Mu.Lock()
	n := len(*consumers)
	if n <= 0 {
		return nil, false
	}
	for i := range *consumers {
		consumer := (*consumers)[i]
		if consumer.Status == Types.IDLE {
			*consumers = append((*consumers)[:i], (*consumers)[i+1:]...)
			*consumers = append(*consumers, consumer)
			fmt.Println("which consumer i found for you", consumer.Conn.RemoteAddr().String())
			return &(*consumers)[len(*consumers)-1], true
		}
	}
	// Constants.Mu.Unlock()
	return nil, false
}
