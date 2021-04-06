package agent

import (
	"log"
	"runtime"
	"strconv"

	"github.com/google/uuid"
	zmq "github.com/pebbe/zmq4"
	"google.golang.org/protobuf/proto"

	mdapi "github.com/Project-Auxo/Olympus/pkg/mdapi"
	mdapi_pb "github.com/Project-Auxo/Olympus/proto/mdapi"
)


// Expose LoadService from service package.
var LoadService func(
	*Worker, string, *mdapi_pb.WrapperCommand) *mdapi_pb.WrapperCommand


// TODO: Replace all "worker" with "worker" to get consistency across codebase.


type Args struct {
	WorkerIdentity uuid.UUID	// To send to specific worker.
	ServiceName string
	ReplyAddress string		// Address of who to address the message to.
	Forward bool // If the message being sent was forwarded.
}

// Worker is the Majordomo Protocol Worker API.
type Worker struct {
	id uuid.UUID
	service string
	coordinator string	// Where to connect to coordinator.
	coordinatorSocket *zmq.Socket	// Socket to coordinator.
	verbose bool	// Print activity to stdout.
}


// NewWorker is a constructor.
func NewWorker(
	id uuid.UUID,
	coordinator string, verbose bool, identity string) (worker *Worker, err error) {
		worker = &Worker{
			id: id,
			coordinator: coordinator,
			verbose: verbose,
		}
		err = worker.ConnectToCoordinator(identity)

		readyProto, _ := worker.PackageProto(
			mdapi_pb.CommandTypes_READY, []string{}, Args{})
		worker.SendToCoordinator(readyProto)

		runtime.SetFinalizer(worker, (*Worker).Close)
		return
}

func (worker *Worker) GetID() uuid.UUID {
	return worker.id
}

// Close is worker's destructor.
func (worker *Worker) Close() {
	if worker.coordinatorSocket != nil {
		worker.coordinatorSocket.Close()
		worker.coordinatorSocket = nil
	}
}


func (worker *Worker) ConnectToCoordinator(identity string) (err error) {
	worker.Close()
	worker.coordinatorSocket, _ = zmq.NewSocket(zmq.DEALER)
	if identity != "" {
		worker.coordinatorSocket.SetIdentity(identity)
	}
	err = worker.coordinatorSocket.Connect(worker.coordinator)
	if worker.verbose {
		log.Printf(
			"W-%s: connecting to coordinator at %s\n",
			worker.id.String(), worker.coordinator)
	}
	return
}


// PackageProto will marshal the given information into the correct bytes
// package.
func (worker *Worker) PackageProto(
	commandType mdapi_pb.CommandTypes, msg []string,
	args Args) (msgProto *mdapi_pb.WrapperCommand, err error) {
		msgProto = &mdapi_pb.WrapperCommand{
			Header: &mdapi_pb.Header{
				Type: commandType,
				Entity: mdapi_pb.Entities_WORKER,
				Origin: worker.id.String(),
				Address: worker.id.String(),
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
func (worker *Worker) SendToCoordinator(
	msgProto *mdapi_pb.WrapperCommand) (err error) {
		commandType := msgProto.GetHeader().GetType()
		msgBytes, err := proto.Marshal(msgProto)
		if err != nil {
			panic(err)
		}
	
		if worker.verbose {
		log.Printf("W-%s: send %s to coordinator\n", worker.id.String(),
			mdapi.CommandMap[commandType])
		}
		_, err = worker.coordinatorSocket.SendMessage(msgBytes)
		return
}


func (worker *Worker) RecvFromCoordinator() (msgProto *mdapi_pb.WrapperCommand) {
	// recvBytes of form: [actorIdentity, forwaded bit, msgPayload]
	recvBytes, _ := worker.coordinatorSocket.RecvMessageBytes(0)
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

	if worker.verbose {
		log.Printf("W-%s: received message from coordinator: %q\n",
		worker.id.String(), msgProto)
	}

	command := msgProto.GetHeader().GetType()
	switch command {
	case mdapi_pb.CommandTypes_REQUEST:
		worker.service = msgProto.GetRequest().GetServiceName()
		return
	case mdapi_pb.CommandTypes_DISCONNECT:
		// Means the worker should shut down.
		if worker.verbose {
			log.Printf("W-%s killing self", worker.id.String())
		}
		worker.Close()
	default:
		log.Printf("E: invalid input message %q\n", command)
	}
	return nil
}


func (worker *Worker) Work() {
	for {
		recvProto := worker.RecvFromCoordinator()
		if recvProto.GetHeader().GetType() != mdapi_pb.CommandTypes_REQUEST {
			return
			// panic("E: not a request.")
		}
		serviceName := recvProto.GetRequest().GetServiceName()
		replyProto := LoadService(worker, serviceName, recvProto)
		worker.SendToCoordinator(replyProto)
	}
}