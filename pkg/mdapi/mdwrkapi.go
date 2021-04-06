package mdapi

import (
	"log"
	"runtime"
	"strconv"

	"github.com/google/uuid"
	zmq "github.com/pebbe/zmq4"
	"google.golang.org/protobuf/proto"

	// "github.com/Project-Auxo/Olympus/pkg/service"
	mdapi_pb "github.com/Project-Auxo/Olympus/proto/mdapi"
)

// TODO: Replace all "mdwrk" with "worker" to get consistency across codebase.


type Args struct {
	WorkerIdentity uuid.UUID	// To send to specific worker.
	ServiceName string
	ReplyAddress string		// Address of who to address the message to.
	Forward bool // If the message being sent was forwarded.
}

// Mdwrk is the Majordomo Protocol Worker API.
type Mdwrk struct {
	id uuid.UUID
	service string
	coordinator string	// Where to connect to coordinator.
	coordinatorSocket *zmq.Socket	// Socket to coordinator.
	verbose bool	// Print activity to stdout.
}


// NewMdwrk is a constructor.
func NewMdwrk(
	id uuid.UUID,
	coordinator string, verbose bool, identity string) (mdwrk *Mdwrk, err error) {
		mdwrk = &Mdwrk{
			id: id,
			coordinator: coordinator,
			verbose: verbose,
		}
		err = mdwrk.ConnectToCoordinator(identity)

		readyProto, _ := mdwrk.PackageProto(
			mdapi_pb.CommandTypes_READY, []string{}, Args{})
		mdwrk.SendToCoordinator(readyProto)

		runtime.SetFinalizer(mdwrk, (*Mdwrk).Close)
		return
}

func (mdwrk *Mdwrk) GetID() uuid.UUID {
	return mdwrk.id
}

// Close is mdwrk's destructor.
func (mdwrk *Mdwrk) Close() {
	if mdwrk.coordinatorSocket != nil {
		mdwrk.coordinatorSocket.Close()
		mdwrk.coordinatorSocket = nil
	}
}


func (mdwrk *Mdwrk) ConnectToCoordinator(identity string) (err error) {
	mdwrk.Close()
	mdwrk.coordinatorSocket, _ = zmq.NewSocket(zmq.DEALER)
	if identity != "" {
		mdwrk.coordinatorSocket.SetIdentity(identity)
	}
	err = mdwrk.coordinatorSocket.Connect(mdwrk.coordinator)
	if mdwrk.verbose {
		log.Printf(
			"W-%s: connecting to coordinator at %s\n",
			mdwrk.id.String(), mdwrk.coordinator)
	}
	return
}


// PackageProto will marshal the given information into the correct bytes
// package.
func (mdwrk *Mdwrk) PackageProto(
	commandType mdapi_pb.CommandTypes, msg []string,
	args Args) (msgProto *mdapi_pb.WrapperCommand, err error) {
		msgProto = &mdapi_pb.WrapperCommand{
			Header: &mdapi_pb.Header{
				Type: commandType,
				Entity: mdapi_pb.Entities_WORKER,
				Origin: mdwrk.id.String(),
				Address: mdwrk.id.String(),
			},
		}

		switch commandType {
		case mdapi_pb.CommandTypes_READY:
			msgProto.Command = &mdapi_pb.WrapperCommand_Ready{
				Ready: &mdapi_pb.Ready{},
			}
		case mdapi_pb.CommandTypes_REPLY:
			serviceName := args.ServiceName
			replyAddress := args.ReplyAddress
			msgProto.Command = &mdapi_pb.WrapperCommand_Reply{
				Reply: &mdapi_pb.Reply{
					ServiceName: serviceName,
					ReplyAddress: replyAddress,
					ReplyBody: &mdapi_pb.Reply_Body{Body: &mdapi_pb.Body{Body: msg},},
				},
			}
		default:
			log.Fatalf("E-W: uknown commandType %q", commandType)
		}
		return
}

// SendToCoordinator sends a proto to the coordinator.
func (mdwrk *Mdwrk) SendToCoordinator(
	msgProto *mdapi_pb.WrapperCommand) (err error) {
		commandType := msgProto.GetHeader().GetType()
		msgBytes, err := proto.Marshal(msgProto)
		if err != nil {
			panic(err)
		}
	
		if mdwrk.verbose {
		log.Printf("W-%s: send %s to coordinator\n", mdwrk.id.String(),
			CommandMap[commandType])
		}
		_, err = mdwrk.coordinatorSocket.SendMessage(msgBytes)
		return
}


func (mdwrk *Mdwrk) RecvFromCoordinator() (msgProto *mdapi_pb.WrapperCommand) {
	// recvBytes of form: [actorIdentity, forwaded bit, msgPayload]
	recvBytes, _ := mdwrk.coordinatorSocket.RecvMessageBytes(0)
	forwarded, _ := strconv.Atoi(string(recvBytes[1]))
	bytesPayload := recvBytes[2]

	var fromEntity mdapi_pb.Entities
	if forwarded == 1 {
		forwardedProto := &mdapi_pb.ForwardedCommand{}
		if err := proto.Unmarshal(bytesPayload, forwardedProto); err != nil {
			log.Fatalln("E-W: failed to parse forwaded command:", err)
		}
		msgProto = forwardedProto.GetForwardedCommand()
		fromEntity = forwardedProto.GetHeader().GetEntity()
	}else {
		msgProto = &mdapi_pb.WrapperCommand{}
		if err := proto.Unmarshal(bytesPayload, msgProto); err != nil {
			log.Fatalln("E-W: failed to parse wrapper command:", err)
		}	
		fromEntity = msgProto.GetHeader().GetEntity()
	}

	// Don't try to handle errors, just assert noisily.
	if fromEntity != mdapi_pb.Entities_ACTOR {
		log.Panicf("E: received message is not from a coordinator: %q", fromEntity)
	}

	if mdwrk.verbose {
		log.Printf("W-%s: received message from coordinator: %q\n",
		mdwrk.id.String(), msgProto)
	}

	command := msgProto.GetHeader().GetType()
	switch command {
	case mdapi_pb.CommandTypes_REQUEST:
		return
	case mdapi_pb.CommandTypes_DISCONNECT:
		// Means the worker should shut down.
		if mdwrk.verbose {
			log.Printf("W-%s killing self", mdwrk.id.String())
		}
		mdwrk.Close()
	default:
		log.Printf("E: invalid input message %q\n", command)
	}
	return nil
}


func (mdwrk *Mdwrk) Work() {
	for {
		recvProto := mdwrk.RecvFromCoordinator()
		if recvProto.GetHeader().GetType() != mdapi_pb.CommandTypes_REQUEST {
			return
			// panic("E: not a request.")
		}
		mdwrk.service = recvProto.GetRequest().GetServiceName()
		// replyAddress := recvProto.GetHeader().GetAddress()
		// FIXME: Should be using service.LoadService, but running into import cycle
		// issue.
		// replyProto := service.LoadService(mdwrk, mdwrk.service, recvProto)

		// FIXME: Delete me.
		replyAddress := recvProto.GetHeader().GetAddress()
		replyProto, _ := mdwrk.PackageProto(mdapi_pb.CommandTypes_REPLY,
			[]string{"Hello from worker"},
			Args{ServiceName: mdwrk.service, ReplyAddress: replyAddress})
		mdwrk.SendToCoordinator(replyProto)
	}
}