package main

import (
	"fmt"
	"time"

	zmq "github.com/pebbe/zmq4"
	"google.golang.org/protobuf/proto"

	pb "github.com/Project-Auxo/Olympus/proto/mdapi"
)

var addr string = "tcp://127.0.0.1:5556"

func main() {
	socket, _ := zmq.NewSocket(zmq.REQ)
	defer socket.Close()
	if err := socket.Connect(addr); err != nil {
		panic(err)
	}
	fmt.Printf("client: connected to address %s\n", addr)

	for {
		request := &pb.WrapperCommand{
			Header: &pb.Header{
				Type: pb.CommandTypes_REQUEST,
				Entity: pb.Entities_CLIENT,
				Origin: "client1",
				Address: "USA",
			},
			Command: &pb.WrapperCommand_Request{
				Request: &pb.Request{
					ServiceName: "echo",
					RequestBody: &pb.Body{Body: []string{"hello", "world"},},
				},
			},
		}
		reqBytes, err := proto.Marshal(request)
		if err != nil {
			panic(err)
		}
		socket.SendMessage(reqBytes)
		time.Sleep(2 * time.Second)
	}
}