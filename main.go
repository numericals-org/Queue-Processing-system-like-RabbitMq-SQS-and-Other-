package main

import (
	"fmt"
	"log"
	"net"
	"time"

	Broker "github.com/numericals/queueSys/broker"
	"github.com/numericals/queueSys/storage"
)

func main() {

	ln, err := net.Listen("tcp", ":6464")

	if err != nil {
		fmt.Println("TCP connection issue", err)
	}

	wal, err := storage.NewWal("data/wal.log")

	if err != nil {
		fmt.Println(err)
	}

	Broker := Broker.Broker{
		Notify:             make(chan bool),
		MaxDeliveryAttempt: 3,
		VisibilityTimeout:  30,
		DefaultRetryDelay:  5 * time.Second,
		Storage:            wal,
	}

	go Broker.Dispatcher()
	go Broker.VisibilityWatcher()

	for {
		conn, err := ln.Accept()

		if err != nil {
			log.Println("Get issue while getting the conn info", err)
		}

		fmt.Println("our server getting connection")

		go Broker.Receiver(conn)
	}

}
