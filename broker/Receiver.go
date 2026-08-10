package broker

import (
	"encoding/json"
	"fmt"
	"log"
	"net"

	"github.com/numericals/queueSys/types"
)

func (b *Broker) Receiver(Conn net.Conn) {
	buffer := make([]byte, 1024)
	defer b.Wg.Done()

	for {

		length, err := Conn.Read(buffer)
		if err != nil {
			b.HandleDisconnect(Conn)
			return
		}
		var packet types.Packet
		err = json.Unmarshal(buffer[:length], &packet)
		if err != nil {
			log.Println("unable to Unmarshal the json", err)
			continue
		}

		switch packet.Type {
		case types.REGISTER_P:
			b.RegisterProducer(Conn)
		case types.REGISTER_C:
			err = b.RegisterConsumer(packet.Payload, Conn)
		case types.CREATE_QUEUE:
			err = b.RegisterQueue(packet.Payload)
		case types.DELETE_QUEUE:
			err = b.UnregisterQueue(packet.Payload)
		case types.PUBLISH:
			err = b.Publish(packet.Payload)
		case types.NACK:
			err = b.Nack(packet.Payload, Conn)
		case types.ACK:
			err = b.Ack(packet.Payload, Conn)
		default:
			err = fmt.Errorf("unknown packet type: %d", packet.Type)
		}

		if err != nil {
			log.Println(err)
		}
	}
}
