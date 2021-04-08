package echo

import (
	"fmt"
	"log"

	"google.golang.org/protobuf/types/known/anypb"

	agent "github.com/Project-Auxo/Olympus/pkg/agent"
	mdapi_pb "github.com/Project-Auxo/Olympus/proto/mdapi"
	echo_pb "github.com/Project-Auxo/Olympus/proto/service/echo"
)

// TODO: This should take an echo proto.
/*
Request:
	-

Response:
	-
*/

// ------------ Client --------------------

func GenerateEchoRequest(msg []string) *anypb.Any {
	echoProto := &echo_pb.EchoRequest{Request: msg}
	echoAny, err := anypb.New(echoProto)
	if err != nil {
		panic("failed to generate an example proto")
	}
	return echoAny
}

func ClientRequest(
	client *agent.Client, requestProto *mdapi_pb.WrapperCommand) { 
		var count int
		numTries := 1
		for count = 0; count < numTries; count++ {
			err := client.SendToBroker(requestProto)
			if err != nil {
				log.Println("Send:", err)
				break
			}
		}
		for count = 0; count < numTries; count++ {
			recvProto := client.RecvFromBroker()
			log.Println(recvProto)
		}
		log.Printf("%d replies received\n", count)
}


// ------------- Actor -------------------
// Recall that the worker is controlled by the actor's internal coordinator,
// i.e. worker's response is handed-off to its respective coordinator.
func ActorResponse(worker *agent.Worker, requestProto *mdapi_pb.WrapperCommand) (
	replyProto *mdapi_pb.WrapperCommand) {

		replyAddress := requestProto.GetHeader().GetAddress()
		service := requestProto.GetRequest().GetServiceName()

		var msg []string
		switch requestProto.GetRequest().GetRequestBody().(type) {
		case *mdapi_pb.Request_Body:
			msg = requestProto.GetRequest().GetBody().GetBody()
		case *mdapi_pb.Request_CustomBody:
			echo := &echo_pb.EchoRequest{}
			echoAny := requestProto.GetRequest().GetCustomBody()
			if err := echoAny.UnmarshalTo(echo); err != nil {
				log.Println("echo: failed to unmarshal request")
			}
			msg = echo.GetRequest()
		}
		
		msg = append(msg, fmt.Sprintf("from %s", worker.GetID().String()))
		replyProto, _ = worker.PackageProto(mdapi_pb.CommandTypes_REPLY, msg,
			agent.Args{ServiceName: service, ReplyAddress: replyAddress})
		return
}