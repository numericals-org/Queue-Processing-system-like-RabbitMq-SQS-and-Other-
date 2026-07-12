package main

import (
	"fmt"
	"log"
	"net"

	Broker "github.com/numericals/queueSys/broker"
)

func main() {

	ln, err := net.Listen("tcp", ":6464")
	Broker := Broker.Broker{
		Notify:             make(chan bool),
		MaxDeliveryAttempt: 3,
	}

	if err != nil {
		fmt.Println("TCP connection issue", err)
	}

	go Broker.Dispatcher()

	for {
		conn, err := ln.Accept()

		if err != nil {
			log.Println("Get issue while getting the conn info", err)
		}

		fmt.Println("our server getting connection")

		go Broker.Receiver(conn)
	}

}
