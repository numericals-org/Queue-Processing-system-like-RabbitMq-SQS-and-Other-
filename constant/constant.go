package constant

import (
	types "github.com/numericals/queueSys/types"
)

var Producers []types.Producer

var Consumer []types.Consumer

var Message []types.Message

var Notify = make(chan bool)
