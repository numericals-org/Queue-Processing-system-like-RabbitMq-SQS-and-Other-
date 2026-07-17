package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"

	types "github.com/numericals/queueSys/types"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:6464")
	if err != nil {
		fmt.Println(err.Error())
	}

	role := &types.Packet{
		Content: []byte("Register as producer"),
		Type:    types.REGISTER_P,
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

	// fmt.Println("enter your message")
	// fmt.Scanf("%s", &str)
	// fmt.Println(str)
	var Text string = ""

	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("Enter text: ")
		text, _ := reader.ReadString('\n')
		Text = text

		json_msg := &types.Packet{
			Content: []byte(Text),
			Type:    types.QUEUE,
		}

		payload, err := json.Marshal(json_msg)
		if err != nil {
			log.Fatal(err)
			return
		}

		_, err = conn.Write([]byte(payload))
		if err != nil {
			log.Fatal(err.Error())
		}
		// fmt.Println(val)
		fmt.Println(&conn, "GET / HTTP/1.0\r\n\r\n")
	}

	// ([]byte("Here is a string...."))
}
