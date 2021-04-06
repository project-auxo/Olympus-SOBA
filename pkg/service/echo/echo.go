package echo

import (
	"fmt"
	"log"

	agent "github.com/Project-Auxo/Olympus/pkg/agent"
	mdapi_pb "github.com/Project-Auxo/Olympus/proto/mdapi"
)

// TODO: This should take an echo proto.
/*
Request:
	-

Response:
	-
*/


// ------------ Client --------------------
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
		msg := requestProto.GetRequest().GetBody().GetBody()
		msg = append(msg, fmt.Sprintf("from %s", worker.GetID().String()))
		replyProto, _ = worker.PackageProto(mdapi_pb.CommandTypes_REPLY, msg,
			agent.Args{ServiceName: service, ReplyAddress: replyAddress})

		return
}