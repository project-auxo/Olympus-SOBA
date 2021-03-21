package mdapi

const (
	// MdpcClient is the implementation of MDP/Client v.01
	MdpcClient = "MDPC01"

	// MdpWorker is the implementation of MDP/Worker v.01
	MdpWorker = "MDPW01"

	// MdpActor is the implementation of SOBA Actor v.01
	MdpActor = "SOBAA01"
)

// MDP/Server commands, as strings.
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
