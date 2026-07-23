package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"time"

	Broker "github.com/numericals/queueSys/broker"
	"github.com/numericals/queueSys/service"
	"github.com/numericals/queueSys/storage"
	"github.com/numericals/queueSys/types"
	"github.com/numericals/queueSys/utils"
)

func main() {

	ln, err := net.Listen("tcp", ":6464")

	if err != nil {
		fmt.Println("TCP connection issue", err)
	}

	wal, err := storage.NewWal("data/wal/", "data/snapshot/snapshot.bin", "data/snapshot/snapshot.bin.temp")

	if err != nil {
		fmt.Println(err)
	}

	Broker := Broker.Broker{
		Notify:             make(chan bool),
		MaxDeliveryAttempt: 3,
		VisibilityTimeout:  30,
		DefaultRetryDelay:  30 * time.Second,
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

	files, err := os.ReadDir("data/wal/")

	if err != nil {
		log.Println("issue in reading directory", err)
	}

	sort.Slice(files, func(i, j int) bool {
		file_1, err := files[i].Info()

		if err != nil {
			log.Println("issue in reading file no :=", i, ",", err)
		}

		file_2, err := files[j].Info()

		if err != nil {
			log.Println("issue in reading file no :=", j, ",", err)
		}
		return utils.ExtractNumber(file_1.Name()) < utils.ExtractNumber(file_2.Name())
	})

	var events []types.WALEvent
	var highestNumber uint64

	for _, file := range files {
		event, highest, err := Broker.Storage.Replay(Broker.LastAppliedEventID, file.Name())

		if err != nil {
			log.Println("issue in reading file", err)
			continue
		}

		events = append(events, event...)
		if highest > highestNumber {
			highestNumber = highest
		}
	}

	wal.NextEventID = highestNumber + 1

	for _, event := range events {
		Broker.Apply(event)
	}

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
