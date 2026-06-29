package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
)

type connections struct {
	Producers []net.Conn
	Consumers []net.Conn
}

type Role struct {
	Role string `json:"role"`
}

func Test() {
	ln, err := net.Listen("tcp", ":4000")
	c := make(chan []byte)
	var connections connections
	var MsgQueue []any

	if err != nil {
		log.Fatal(err.Error())
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal(err.Error())
		}

		go ReadValue(conn, c, &connections)
		go SendValue(c, &connections)

	}
}

func ReadValue(conn net.Conn, c chan []byte, connections *connections) {
	buffer := make([]byte, 1024)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			log.Fatal(err.Error())
		}
		var msg Role
		err = json.Unmarshal(buffer[:n], &msg)
		if err != nil {
			fmt.Println("error:", err)
			c <- buffer[:n]
		} else {
			if msg.Role == "producer" {
				connections.Producers = append(connections.Producers, conn)
			} else {
				connections.Consumers = append(connections.Consumers, conn)
			}
		}
	}
}

func SendValue(c chan []byte, connections *connections) {
	for {
		val := <-c

		for _, consumer := range connections.Consumers {
			consumer.Write(val)
		}
	}
}
