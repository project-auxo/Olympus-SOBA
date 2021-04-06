package mdapi

import mdapi_pb "github.com/Project-Auxo/Olympus/proto/mdapi"


var (
	EntitiesMap = map[mdapi_pb.Entities]string{
		mdapi_pb.Entities_BROKER:		"BROKER",
		mdapi_pb.Entities_ACTOR: 		"ACTOR",
		mdapi_pb.Entities_CLIENT: 	"CLIENT",
		mdapi_pb.Entities_WORKER: 	"WORKER",
	}
)

var (
	CommandMap = map[mdapi_pb.CommandTypes]string{
		mdapi_pb.CommandTypes_READY:      "READY",
		mdapi_pb.CommandTypes_REQUEST:    "REQUEST",
		mdapi_pb.CommandTypes_REPLY:      "REPLY",
		mdapi_pb.CommandTypes_HEARTBEAT:  "HEARTBEAT",
		mdapi_pb.CommandTypes_DISCONNECT: "DISCONNECT",
	}
)
