package main

import (
	"fmt"
	"log"
	"net"

	Services "github.com/numericals/queueSys/services"
)

func main() {
	ln, err := net.Listen("tcp", ":6464")

	if err != nil {
		fmt.Println("TCP connection issue", err)
	}

	go Services.Dispatcher()

	for {
		conn, err := ln.Accept()

		if err != nil {
			log.Fatal("Get issue while getting the conn info", err)
		}

		fmt.Println("our server getting connection")

		go Services.Receiver(conn)
	}

}
