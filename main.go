package main

import (
	"fmt"
	"log"
	"net"
	"time"

	Broker "github.com/numericals/queueSys/broker"
	"github.com/numericals/queueSys/service"
	"github.com/numericals/queueSys/storage"
)

func main() {

	ln, err := net.Listen("tcp", ":6464")

	if err != nil {
		fmt.Println("TCP connection issue", err)
	}

	wal, err := storage.NewWal("data/wal/wal.log", "data/snapshot/snapshot.bin")

	if err != nil {
		fmt.Println(err)
	}

	Broker := Broker.Broker{
		Notify:             make(chan bool),
		MaxDeliveryAttempt: 3,
		VisibilityTimeout:  30,
		DefaultRetryDelay:  5 * time.Second,
		Storage:            wal,
		SnapshotNotify:     make(chan struct{}, 1),
	}

	SnapshotManager := service.NewSnapshotManager(wal, &Broker)

	snap, err := wal.LoadSnapshot()

	if err != nil {
		log.Println("issue in reading file", err)
		return
	}

	Broker.ApplySnapshot(snap)

	events, highestNumber, err := Broker.Storage.Replay(Broker.LastAppliedEventID)

	wal.NextEventID = highestNumber + 1

	if err != nil {
		log.Println("issue in reading file", err)
		return
	}

	for _, event := range events {
		Broker.Apply(event)
	}
	fmt.Println("working at line number 59")

	go Broker.RecoverInFlightMessages()

	go SnapshotManager.Start()

	go Broker.Dispatcher()
	go Broker.VisibilityWatcher()
	go Broker.RetryWatcher()

	for {
		conn, err := ln.Accept()

		if err != nil {
			log.Println("Get issue while getting the conn info", err)
		}

		fmt.Println("our server getting connection")

		go Broker.Receiver(conn)
	}

}
