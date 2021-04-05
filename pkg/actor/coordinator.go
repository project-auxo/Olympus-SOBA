package actor

import (
	"log"
	"strconv"
	"time"

	"github.com/google/uuid"
	zmq "github.com/pebbe/zmq4"
	"google.golang.org/protobuf/proto"

	"github.com/Project-Auxo/Olympus/pkg/mdapi"
	"github.com/Project-Auxo/Olympus/pkg/util"
	mdapi_pb "github.com/Project-Auxo/Olympus/proto/mdapi"
)

/*

          broker
            ^
            |									(Broker Interface)
				(dealer)
	 ____coordinator____
	|     (router)      |       (Worker Interface)
  |         |         |
(dealer)   ...       ...
worker		worker 		worker

*/

const (
	heartbeatLiveness = 3
)

const (
	workersEndpoint = "inproc://workers"
)


type Coordinator struct {
	actorName string

	brokerSocket *zmq.Socket		// Interface with the broker.
	workerSocket *zmq.Socket		// Interface with the internal workers.
	broker string 	// Coordinator connects to broker through this endpoint.
	endpoint string // Coordinator binds to this endpoint.
	poller *zmq.Poller

	runningWorkers map[string]*mdapi.Mdwrk

	verbose bool 	// Print activity to stdout
	// services map[string]*mdapi.Mdwrk		// Hash of current running services.
	loadableServices []string

	// Heartbeat management.
	heartbeatAt time.Time // When to send heartbeat.
	liveness int	// How many attempts left.
	heartbeat time.Duration 	// Heartbeat delay, msecs.
	reconnect time.Duration // Reconnect delay, msecs.
}


// NewCoordinator is the constructor for the coordinator.
func NewCoordinator(
	actorName string,
	broker string,
	endpoint string,
	loadableServices []string,
	verbose bool) (coordinator *Coordinator, err error) {
	// Initialize broker state.
	coordinator = &Coordinator{
		actorName: actorName,
		broker: broker,
		endpoint: endpoint,
		poller: zmq.NewPoller(),
		loadableServices: loadableServices,
		runningWorkers: make(map[string]*mdapi.Mdwrk),
		heartbeat: 2500 * time.Millisecond,
		reconnect: 2500 * time.Millisecond,
		verbose: verbose,
	}
	coordinator.ConnectToBroker()
	coordinator.workerSocket, err = zmq.NewSocket(zmq.DEALER)

	coordinator.poller.Add(coordinator.brokerSocket, zmq.POLLIN)
	coordinator.poller.Add(coordinator.workerSocket, zmq.POLLIN)
	return
}


// Binds will bind the coordinator instance to an endpoint. 
func (coordinator *Coordinator) Bind(endpoint string) (err error) {
	err = coordinator.workerSocket.Bind(endpoint)
	if err != nil {
		log.Fatalf("E: coordinator failed to bind at %s", endpoint)
	}
	log.Printf("C: Coordinator is active at %s", endpoint)
	return
}

func (coordinator *Coordinator) Run() {
	for {
		polledSockets, err := coordinator.poller.Poll(coordinator.heartbeat)
		if err != nil {
			panic(err)
		}

		// Handle the broker and worker sockets in turn.
		if len(polledSockets) > 0 {
			for _, socket := range polledSockets {
				switch s := socket.Socket; s {
				case coordinator.brokerSocket:
					coordinator.RecvFromBroker()
				case coordinator.workerSocket:
					coordinator.RecvFromWorkers()
				}
			}
		}else {
			coordinator.liveness--
			if coordinator.liveness == 0 {
				if coordinator.verbose {
					log.Println("C: disconnected from broker, retrying...")
				}
				time.Sleep(coordinator.reconnect)
				coordinator.ConnectToBroker()
			}
		}
		// Send heartbeat if it's time.
		if time.Now().After(coordinator.heartbeatAt) {
			heartbeatProto, _ := coordinator.PackageProto(
				mdapi_pb.CommandTypes_HEARTBEAT, []string{}, mdapi.Args{})
			coordinator.SendToEntity(heartbeatProto, mdapi_pb.Entities_BROKER,
				mdapi.Args{})
			coordinator.heartbeatAt = time.Now().Add(coordinator.heartbeat)
		}
	}
}

func (coordinator *Coordinator) Close() {
	if coordinator.brokerSocket != nil {
		coordinator.brokerSocket.Close()
		coordinator.brokerSocket = nil
	}
}


// ----------------- Broker Interface ------------------------

// PackageProto will marshal the given information into the correct bytes
// package.
func (coordinator *Coordinator) PackageProto(
	commandType mdapi_pb.CommandTypes, msg []string,
	args mdapi.Args) (msgProto *mdapi_pb.WrapperCommand, err error){
		msgProto = &mdapi_pb.WrapperCommand{
			Header: &mdapi_pb.Header{
				Type: commandType,
				Entity: mdapi_pb.Entities_ACTOR,
				Origin: coordinator.actorName,
				Address: "Nil",
			},
		}

		switch commandType {
		case mdapi_pb.CommandTypes_READY:
			msgProto.Command = &mdapi_pb.WrapperCommand_Ready{
				Ready: &mdapi_pb.Ready{AvailableServices: coordinator.loadableServices},
			}
		case mdapi_pb.CommandTypes_REQUEST:
			serviceName := args.ServiceName
			msgProto.Command = &mdapi_pb.WrapperCommand_Request{
				Request: &mdapi_pb.Request{
					ServiceName: serviceName,
					RequestBody: &mdapi_pb.Request_Body{Body: &mdapi_pb.Body{Body: msg},},
				},
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
		case mdapi_pb.CommandTypes_HEARTBEAT:
			msgProto.Command = &mdapi_pb.WrapperCommand_Heartbeat{
				Heartbeat: &mdapi_pb.Heartbeat{
					AvailableServices: coordinator.loadableServices,
				},
			}
		case mdapi_pb.CommandTypes_DISCONNECT:
			// FIXME: Insert the expiration time here.
			msgProto.Command = &mdapi_pb.WrapperCommand_Disconnect{}
		default:
			log.Fatalf("E: uknown commandType %q", commandType)
		}
		return
}

// ForwardProto will forward a mdapi_pb.WrapperCommand while maintaining the
// broker's header.
func (coordinator *Coordinator) ForwardProto(
	msgProto *mdapi_pb.WrapperCommand) (forwadedProto *mdapi_pb.ForwardedCommand) {
		forwadedProto = &mdapi_pb.ForwardedCommand{
			Header: &mdapi_pb.Header{
				Type: mdapi_pb.CommandTypes_FORWARDED,
				Entity: mdapi_pb.Entities_ACTOR,
				Origin: coordinator.actorName,
				Address: coordinator.actorName,
			},
		}
		forwadedProto.ForwardedCommand = msgProto
		return
}

// SendToEntity sends a message to the specified entity.
func (coordinator *Coordinator) SendToEntity(msgProto *mdapi_pb.WrapperCommand,
	entity mdapi_pb.Entities, args mdapi.Args) (err error) {
		commandType := msgProto.GetHeader().GetType()
		var msgBytes []byte
		if args.Forward {
			msgBytes, err = proto.Marshal(coordinator.ForwardProto(msgProto))
		}else {
			msgBytes, err = proto.Marshal(msgProto)
		}
		if err != nil {
			panic(err)
		}
		if coordinator.verbose {
			forwardMap := map[bool]string{false: "sending", true: "forwarding"}
			log.Printf("C: %s %s to %s\n", forwardMap[args.Forward], mdapi.
			CommandMap[commandType], mdapi.EntitiesMap[entity])
		}
		switch entity {
		case mdapi_pb.Entities_BROKER:
			_, err = coordinator.brokerSocket.SendMessage(msgBytes)
		case mdapi_pb.Entities_WORKER:
			// Messages to workers can be forwaded, so include as option in
			// SendMessage.
			byteId, _ := args.WorkerIdentity.MarshalBinary()
			_, err = coordinator.workerSocket.SendMessage(
				byteId, util.Btou(args.Forward), msgBytes)
		default:
			log.Fatal("E: unrecognized entity")
		}
		return
}

// ConnectToBroker attempts to connect or reconnect to the broker.
func (coordinator *Coordinator) ConnectToBroker() (err error) {
	if coordinator.brokerSocket != nil {
		coordinator.brokerSocket.Close()
		coordinator.brokerSocket = nil
	}
	coordinator.brokerSocket, _ = zmq.NewSocket(zmq.DEALER)
	coordinator.brokerSocket.Connect(coordinator.broker)
	if coordinator.verbose {
		log.Printf("C: connecting to broker at %s...\n", coordinator.broker)
	}
	coordinator.poller = zmq.NewPoller()
	coordinator.poller.Add(coordinator.brokerSocket, zmq.POLLIN)

	// Register coordinator with the broker.
	readyProto, err := coordinator.PackageProto(
		mdapi_pb.CommandTypes_READY, []string{}, mdapi.Args{})
	coordinator.SendToEntity(readyProto, mdapi_pb.Entities_BROKER, mdapi.Args{})

	// If liveness hits zero, queue is considered disconnected.
	coordinator.liveness = heartbeatLiveness
	coordinator.heartbeatAt = time.Now().Add(coordinator.heartbeat)

	return
}

// RecvFromBroker waits for the next request.
func (coordinator *Coordinator) RecvFromBroker() {
	var msgProto *mdapi_pb.WrapperCommand
	recvBytes, err := coordinator.brokerSocket.RecvMessageBytes(0)
	forwarded, _ := strconv.Atoi(string(recvBytes[0]))	// Broker sends a bit saying whether this
															// message was forwaded from elsewhere.
	if err != nil {
		panic(err) 
	}
	coordinator.liveness = heartbeatLiveness

	var fromEntity mdapi_pb.Entities
	// Try unmarshalling as WrapperCommand first, if error, try unmarshaling
	// as ForwardedCommand. The resultant msgProto will always be
	// WrapperCommand, i.e. will never receive forwaded-forwaded message.
	if forwarded == 1 {
		forwardedProto := &mdapi_pb.ForwardedCommand{}
		if err = proto.Unmarshal(recvBytes[1], forwardedProto); err != nil {
			log.Fatalln("E: failed to parse forwarded command:", err)
		}
		msgProto = forwardedProto.GetForwardedCommand()
		fromEntity = forwardedProto.GetHeader().GetEntity()
	}else {
		msgProto = &mdapi_pb.WrapperCommand{}
		if proto.Unmarshal(recvBytes[1], msgProto); err != nil {
			log.Fatalln("E: failed to parse for wrapped command:", err)
		}
		fromEntity = msgProto.GetHeader().GetEntity()
	}

	if coordinator.verbose {
		log.Printf("C: received message from broker: %q\n", msgProto)
	}

	// Don't try to handle errors, just assert noisily.
	if fromEntity != mdapi_pb.Entities_BROKER {
		panic("E: received message is not from a broker.")
	}

	command := msgProto.GetHeader().GetType()
	switch command {
	case mdapi_pb.CommandTypes_REQUEST:
		// We have a request to process.
		coordinator.DispatchRequests(msgProto)
	case mdapi_pb.CommandTypes_HEARTBEAT:
		// Do nothing on heartbeats.
	case mdapi_pb.CommandTypes_DISCONNECT:
		// FIXME: Not sure if the disconnect should correspond to reconnect.
		coordinator.ConnectToBroker()
	default:
		log.Printf("E: invalid input message %q\n", command)
	}
}


// ----------------- Worker Interface ------------------------

func (coordinator *Coordinator) DispatchRequests(
	requestProto *mdapi_pb.WrapperCommand) {
	serviceName := requestProto.GetRequest().GetServiceName()
	id := uuid.New()		/* TODO: Make this a channel so id can be made in the
											 spawn worker function. */ 
	go coordinator.SpawnWorker(id)

	// Forward the requestProto to the worker that was just spawned.

	// Forward the request to the worker.
	coordinator.SendToEntity(requestProto, mdapi_pb.Entities_WORKER, mdapi.Args{
		ServiceName: serviceName, WorkerIdentity: id, Forward: true,})
}

func (coordinator *Coordinator) SpawnWorker(id uuid.UUID) {
		stringId := id.String()
		worker, _ := mdapi.NewMdwrk(
			id, workersEndpoint, coordinator.verbose, stringId)

		// Coordinator waits for worker to register itself.
		// coordinator.workerSocket.RecvMessageBytes(0)
		coordinator.runningWorkers[stringId] = worker
		worker.Work()
}

func (coordinator *Coordinator) KillWorker(id uuid.UUID) {
	stringId := id.String()
	_, ok := coordinator.runningWorkers[stringId]
	if !ok {
		log.Printf("E: worker %s does not exist", stringId)
		return
	}
	disconnectProto, _ := coordinator.PackageProto(
		mdapi_pb.CommandTypes_DISCONNECT, []string{}, mdapi.Args{})
	coordinator.SendToEntity(
		disconnectProto, mdapi_pb.Entities_WORKER, mdapi.Args{WorkerIdentity: id})
	delete(coordinator.runningWorkers, stringId)
}

func (coordinator *Coordinator) RecvFromWorkers() {
		recvBytes, _ := coordinator.workerSocket.RecvMessageBytes(0)
		msgProto := &mdapi_pb.WrapperCommand{}
		if err := proto.Unmarshal(recvBytes[0], msgProto); err != nil {
			log.Fatalln("E: failed to parse wrapper command:", err)
		}

		// Don't try to handle errors, just assert noisily.
		if msgProto.GetHeader().GetEntity() != mdapi_pb.Entities_WORKER {
			panic("E: received message is not from a worker.")
		}

		if coordinator.verbose {
			log.Printf("C: received message from worker: %q\n", msgProto)
		}

		command := msgProto.GetHeader().GetType()
		switch command {
		case mdapi_pb.CommandTypes_READY:
			// Do nothing on Ready.
		case mdapi_pb.CommandTypes_REPLY:
			// Work is complete, kill the worker and forward the message back to the
			// broker unchanged.
			id, _ := uuid.Parse(msgProto.GetHeader().GetOrigin())
			coordinator.KillWorker(id)
			coordinator.SendToEntity(
			msgProto, mdapi_pb.Entities_BROKER, mdapi.Args{Forward: true})
		default:
			log.Printf("E: invalid input message %q\n", command)
		}
}
