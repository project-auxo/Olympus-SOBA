package main

import (
	"fmt"
	"log"
	"time"

	zmq "github.com/pebbe/zmq4"
	"google.golang.org/protobuf/proto"

	pb "github.com/Project-Auxo/Olympus/proto/mdapi"
)

var addr string = "tcp://127.0.0.1:5556"

func main() {
	socket, _ := zmq.NewSocket(zmq.REP)
	defer socket.Close()
	socket.Bind(addr)
	fmt.Printf("server: bound address %s\n", addr)

	for {
		in, _ := socket.RecvMessageBytes(zmq.DONTWAIT)
		if len(in) == 0 {
			continue
		}
		fmt.Printf("server received %d parts\n", len(in))
		msg := &pb.WrapperCommand{}
		if err := proto.Unmarshal(in[0], msg); err != nil {
			log.Fatalln("Failed to parse wrapper command:", err)
		}
		fmt.Println("Received!", msg.Header.Type)
		time.Sleep(1 * time.Second)
	}
}