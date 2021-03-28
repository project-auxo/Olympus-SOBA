package mdapi

const (
	// MdpcClient is the implementation of MDP/Client v.01
	MdpcClient = "MDPC01"
	// MdpWorker is the implementation of MDP/Worker v.01
	MdpWorker = "MDPW01"
	// MdpActor is the implementation of SOBA Actor v.01
	MdpActor = "SOBAA01"
)

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
- Frames 7+: Request body

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
const (
	MdpReady = string(iota + 1)
	MdpRequest
	MdpReply
	MdpHeartbeat
	MdpDisconnect
)

// MDP/Server command mappings.
var (
	MdpsCommands = map[string]string{
		MdpReady:      "READY",
		MdpRequest:    "REQUEST",
		MdpReply:      "REPLY",
		MdpHeartbeat:  "HEARTBEAT",
		MdpDisconnect: "DISCONNECT",
	}
)
