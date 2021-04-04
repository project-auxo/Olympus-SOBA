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



// TODO: Make this neater so that documentation parses through it better.
// TODO: Change the order of the serviceName framing.
/* MDP/Server commands, as strings.

-------------------------- Client ----------------------------------
REQUEST command consists of multipart message of 4+ frames:
- Frame 0: Empty
- Frame 1: MdpcClient
- Frame 2: Service name
- Frames 3+: Request body

REPLY command consists of multipart message of 4+ frames:
- Frame 0: Empty
- Frame 1: MdpcClient
- Frame 2: Service name
- Frames 3+: Reply body

--------------------------- Actor ----------------------------------
READY command consists of multipart message of 4+ frames:
- Frame 0: Empty frame
- Frame 1: MdpActor
- Frame 2: MdpReady
- Frame 3+: Services...

REQUEST command consists of a multipart message of 8 or more frames:
- Frame 0: Empty frame
- Frame 1: MdpActor
- Frame 2: mdpRequest
- Frame 3: Service name
- Frame 4: Empty frame
- Frame 5: Client address
- Frame 6: Empty frame
- Frames 7+: Request body

REPLY command consists of a multipart message of 8 or more frames:
- Frame 0: Empty frame
- Frame 1: MdpActor
- Frame 2: mdpReply
- Frame 3: Service name
- Frame 4: Empty frame
- Frame 5: Client address
- Frame 6: Empty frame
- Frames 7+: Reply body

HEARTBEAT command consists of multipart message of 4+ frames:
- Frame 0: Empty frame
- Frame 1: mdpActor
- Frame 2: mdpHeartbeat
- Frame 3+: Services...

DISCONNECT command consists of a multipart message of 3 frames:
- Frame 0: Empty frame
- Frame 1: mdpActor
- Frame 2: mdpDisconnect
*/

var (
	CommandMap = map[mdapi_pb.CommandTypes]string{
		mdapi_pb.CommandTypes_READY:      "READY",
		mdapi_pb.CommandTypes_REQUEST:    "REQUEST",
		mdapi_pb.CommandTypes_REPLY:      "REPLY",
		mdapi_pb.CommandTypes_HEARTBEAT:  "HEARTBEAT",
		mdapi_pb.CommandTypes_DISCONNECT: "DISCONNECT",
	}
)
