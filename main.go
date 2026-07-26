package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"time"

	Broker "github.com/numericals/queueSys/broker"
	"github.com/numericals/queueSys/service"
	"github.com/numericals/queueSys/storage"
	"github.com/numericals/queueSys/types"
	"github.com/numericals/queueSys/utils"
)

func main() {

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	ln, err := net.Listen("tcp", ":6464")

	if err != nil {
		log.Fatal(err)
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
		Ctx:                ctx,
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

	files = utils.SortFilesArray(files)

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
	fmt.Println("i am working")

	Broker.Wg.Add(1)
	go SnapshotManager.Start()

	Broker.Wg.Add(1)
	go Broker.Dispatcher()

	Broker.Wg.Add(1)
	go Broker.VisibilityWatcher()

	Broker.Wg.Add(1)
	go Broker.RetryWatcher()

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Println("accept error:", err)
			continue
		}

		fmt.Println("our server getting connection")

		Broker.Wg.Add(1)
		go Broker.Receiver(conn)
	}

	Broker.Shutdown(SnapshotManager.Flush)
}
