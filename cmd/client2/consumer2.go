package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"

	"github.com/google/uuid"
	"github.com/numericals/queueSys/types"
)

type Role struct {
	Role string `json:"role"`
}

func main() {
	conn, err := net.Dial("tcp", "localhost:6464")
	if err != nil {
		log.Panic(err.Error())
	}

	role := &types.Message{
		MessageId: uuid.New().String(),
		Content:   []byte("Register as consumer"),
		Mtype:     types.REGISTER_C,
	}
	payload, err := json.Marshal(role)
	if err != nil {
		log.Fatal(err)
		return
	}

	_, err = conn.Write(payload)
	if err != nil {
		log.Fatal(err.Error())
	}

	for {
		var msg = make([]byte, 1024)
		n, err := conn.Read(msg)
		if err != nil {
			log.Fatal(err)
			return
		}

		var printVal types.Message

		err = json.Unmarshal(msg[:n], &printVal)
		if err != nil {
			fmt.Println("error:", err)
		}

		fmt.Println(string(printVal.Content))
		if true {
			log.Fatal("man made error for testing")
			return
		}

		role = &types.Message{
			MessageId: uuid.New().String(),
			Content:   []byte("i am available"),
			Mtype:     types.ACKNOWLEDGE,
		}

		payload, err := json.Marshal(role)
		if err != nil {
			log.Fatal(err)
			return
		}

		_, err = conn.Write(payload)
		if err != nil {
			log.Fatal(err.Error())
		}

		fmt.Println(payload)
	}
}
