package mdbroker

import (
	"log"
	"runtime"
	"time"

	zmq "github.com/pebbe/zmq4"
	"google.golang.org/protobuf/proto"

	"github.com/Project-Auxo/Olympus/pkg/util"
	"github.com/Project-Auxo/Olympus/pkg/mdapi"
	mdapi_pb "github.com/Project-Auxo/Olympus/proto/mdapi"
)

/*
           client(s)
							^
              |
              v
						broker
							^
			  ______|________
			 |      |        |
	 coord-1  coord-2  coord-3
    |   |    |   |     ...
		w1  w2   w1  w2    ...
*/


const (
	HeartbeatLiveness = 3
	HeartbeatInterval = 2000 * time.Millisecond
	HeartbeatExpiry = HeartbeatInterval * HeartbeatLiveness
)


func popActor(actors []*Actor) (actor *Actor, actors2 []*Actor) {
	actor = actors[0]
	actors2 = actors[1:]
	return
}

func delActor(actors []*Actor, actor *Actor) []*Actor {
	for i := 0; i < len(actors); i++ {
		if actors[i] == actor {
			actors = append(actors[:i], actors[i+1:]...)
			i--
		}
	}
	return actors
}


// Broker is the Majordomo Protocol Broker, defines a single instance.
type Broker struct {
	socket *zmq.Socket	// Socket for clients and actors.
	verbose bool	// Print activity to stdout.
	endpoint string 	// Broker binds to this endpoint.

	knownServices map[string]struct{}		// Known services on the network.
	runningServices map[string]*Service 		// Hash of running services.
	waitingServices map[string]*Service 	// Service's awaiting an actor.
	
	actorServiceMap map[string][]*Actor		// Actors that can run a service keyed
																				// by serviceName
	actors map[string]*Actor	// Hash of known actors keyed by their identity on
														// socket.

	heartbeatAt 	time.Time	// When to send the heartbeat.
}

// Service defines a running service instance and the actors attached to it.
type Service struct {
	broker *Broker // Broker instance.
	name string		// Service name.
	requests []*mdapi_pb.WrapperCommand		// List of client requests.
	attachedActors []*Actor 	// List of actors attached to this service.

	timestamp time.Time
}

// Actor class defines a single actor, idle or active.
type Actor struct {
	broker *Broker 	// Broker instance.
	identity []byte 	// Identity frame for routing.
	expiry time.Time	// Expires at unless heartbeat.
	availableServices []string
}


// NewBroker is the constructor for the broker.
func NewBroker(verbose bool) (broker *Broker, err error) {
	// Initialize broker state.
	broker = &Broker{
		verbose: verbose,
		runningServices: make(map[string]*Service),
		waitingServices: make(map[string]*Service),
		actorServiceMap: make(map[string][]*Actor),
		knownServices: make(map[string]struct{}),
		actors: make(map[string]*Actor),
		heartbeatAt: time.Now().Add(HeartbeatInterval),
	}
	broker.socket, err = zmq.NewSocket(zmq.ROUTER)
	broker.socket.SetRcvhwm(50000)
	runtime.SetFinalizer(broker, (*Broker).Close)
	return
}

// Close is the destructor for the broker.
func (broker *Broker) Close() (err error) {
	if broker.socket != nil {
		err = broker.socket.Close()
		broker.socket = nil
	}
	return
}


// Bind method binds the broker instance to an endpoint. We can call this
// multiple times. Note that MDP uses a single socket for both the clients and
// actors.
func (broker *Broker) Bind(endpoint string) (err error) {
	err = broker.socket.Bind(endpoint)
	if err != nil {
		log.Println("E: MDP broker failed to bind at", endpoint)
		return
	}
	broker.endpoint = endpoint
	log.Println("I: MDP broker is active at", endpoint)
	return
}

// PackageProto will marshal the given information into the correct bytes
// package.
func (broker *Broker) PackageProto(
	commandType mdapi_pb.CommandTypes, msg []string,
	args mdapi.Args) (msgProto *mdapi_pb.WrapperCommand, err error){
		msgProto = &mdapi_pb.WrapperCommand{
			Header: &mdapi_pb.Header{
				Type: commandType,
				Entity: mdapi_pb.Entities_BROKER,
				Origin: broker.endpoint,
				Address: broker.endpoint,
			},
		}

		switch commandType {
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
				Heartbeat: &mdapi_pb.Heartbeat{},
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
func (broker *Broker) ForwardProto(msgProto *mdapi_pb.WrapperCommand) (
	forwadedProto *mdapi_pb.ForwardedCommand) {
		forwadedProto = &mdapi_pb.ForwardedCommand{
			Header: &mdapi_pb.Header{
				Type: mdapi_pb.CommandTypes_FORWARDED,
				Entity: mdapi_pb.Entities_BROKER,
				Origin: broker.endpoint,
				Address: broker.endpoint,
			},
		}
		forwadedProto.ForwardedCommand = msgProto
		return
}

// addServices will add the services that an actor notifies the broker via its
// Ready and Heartbeat.
func (broker *Broker) addServices(services []string, actor *Actor) {
	actor.availableServices = services
	for _, serviceName := range services {
		broker.knownServices[serviceName] = struct{}{}
	}
	// Update the actorServiceMap.
	for serviceName := range broker.knownServices {
		broker.actorServiceMap[serviceName] = append(
			broker.actorServiceMap[serviceName], actor)
	}
}

// removeServices will remove the given services from the known services of
// the broker.
func (broker *Broker) removeServices(services []string) {
	for _, serviceName := range services {
		delete(broker.knownServices, serviceName)
	}
}

// Handle handles incoming messages to the broker.
func (broker *Broker) Handle() {
	if broker.endpoint == ""{
		panic("Must bind broker first")
	}
	poller := zmq.NewPoller()
	poller.Add(broker.socket, zmq.POLLIN)
 
	// Get and process incoming messages forever or until interrupted.
	for {
		log.Println("...", broker.knownServices)
		polled, err := poller.Poll(HeartbeatInterval)
		if err != nil {
			break		// Interrupted.
		}
		// Process next input message, if any.
		if len(polled) > 0 {
			recvBytes, err := broker.socket.RecvMessageBytes(0)
			if err != nil {
				break	// Interrupted.
			}
			receivedFrom := recvBytes[0]
			msgProto := &mdapi_pb.WrapperCommand{}
			if err = proto.Unmarshal(recvBytes[1], msgProto); err != nil {
				log.Fatalln("E: failed to parse wrapper command:", err)
			}
			
			if broker.verbose {
				log.Printf("I: received message: %q\n", msgProto)
			}

			entity := msgProto.GetHeader().GetEntity()
			switch entity {
			case mdapi_pb.Entities_CLIENT:
			 broker.ClientMsg(msgProto)
			case mdapi_pb.Entities_ACTOR:
			 broker.ActorMsg(receivedFrom, msgProto)
			default:
			 log.Printf("E: invalid message: %q\n", msgProto)
			}
		 }
		 // Disconnect and delete any expired actors, send hearbeats to idle actors
		 // if needed.
		 if time.Now().After(broker.heartbeatAt) {
			 broker.Purge()
			 for _, actor := range broker.actors {
					heartbeatProto, _ := broker.PackageProto(
						mdapi_pb.CommandTypes_HEARTBEAT, []string{}, mdapi.Args{}) 
				 actor.Send(heartbeatProto, false)
			 }
			 broker.heartbeatAt = time.Now().Add(HeartbeatInterval)
		 }
		}
	 log.Println("W: interrupt received, shutting down...")
 }


// ActorMsg processes on Ready, Reply, Heartbeat or Disconnect message sent to
// the broker by an actor.
func (broker *Broker) ActorMsg(sender []byte, msgProto *mdapi_pb.WrapperCommand) {
	command := msgProto.GetHeader().GetType()
	// sender := msgProto.GetHeader().GetOrigin()
	actor := broker.ActorRequire(sender)

	switch command {
	case mdapi_pb.CommandTypes_READY:
		// TODO: Implement MMI services that are handled internally.
		// Actor notifies broker of its services, update broker's internal state.
		availableServices := msgProto.GetHeartbeat().GetAvailableServices()
		broker.addServices(availableServices, actor)
	case mdapi_pb.CommandTypes_REPLY:
		client := msgProto.GetReply().GetReplyAddress()
		msgBytes, _ := proto.Marshal(msgProto)
		broker.socket.SendMessage(client, msgBytes)
	case mdapi_pb.CommandTypes_HEARTBEAT:
		availableServices := msgProto.GetHeartbeat().GetAvailableServices()
		broker.addServices(availableServices, actor)
		actor.expiry = time.Now().Add(HeartbeatExpiry)
	case mdapi_pb.CommandTypes_DISCONNECT:
		broker.DeleteActor(actor, false)
	default:
		log.Println("E: invalid input message")
	}
}

// ClientMsg processes a request coming from a client. We implement MMI requests
// directly here (at present, we implement only the mmi.service request).
// TODO: Implement more of the mmi. internal services.
func (broker *Broker) ClientMsg(msgProto *mdapi_pb.WrapperCommand) {
	command := msgProto.GetHeader().GetType()
	if command != mdapi_pb.CommandTypes_REQUEST {
		log.Fatalln("E: client did not issue a request")
	}
	serviceName := msgProto.GetRequest().GetServiceName()
	service := broker.ServiceRequire(serviceName)

	// TODO: Implement the MMI service requests, which are processed internally.

	service.Dispatch(msgProto)
}


// Purge deletes any idle actors that haven't pinged the broker in a while. 
// Hold actors from oldest to most recent to avoid scanning whenever we find a
// live actor. 
func (broker *Broker) Purge() {
	now := time.Now()
	for _, actor := range broker.actors {
		if actor.expiry.After(now) {
			continue 	// Actor is alive, we're done here
		}
		if broker.verbose {
			log.Printf("I: deleting expired actor %q", actor.identity)
		}
		broker.DeleteActor(actor, false)
	}
}


// Methods that work on a service.

// ServiceRequire is a lazy constructor locates a service by name, or creates a
// new service if there is no service already with that name.
func (broker *Broker) ServiceRequire(serviceName string) (service *Service) {
	service, ok := broker.runningServices[serviceName]
	if !ok {
		service = &Service{
			broker: broker,
			name: serviceName,
			requests: make([]*mdapi_pb.WrapperCommand, 0),
			attachedActors: make([]*Actor, 0),
			timestamp: time.Now(),
		}
		broker.waitingServices[serviceName] = service
		if broker.verbose {
			log.Println("I: added waiting service:", serviceName)
		}
	}
	return
}

// Dispatch sends requests to waiting actors.
func (service *Service) Dispatch(msgProto *mdapi_pb.WrapperCommand) {
	service.requests = append(service.requests, msgProto) // Queue requests.
	service.broker.Purge()
	dispatchableActors := service.broker.actorServiceMap[service.name]

	for len(dispatchableActors) > 0 && len(service.requests) > 0 {
		var actor *Actor
		actor, service.broker.actorServiceMap[service.name] = popActor(
			dispatchableActors)
		
		currMsgProto := service.requests[0]
		service.requests = service.requests[1:]
		// Forward the request to the actor.
		actor.Send(currMsgProto, true)
	}
}


// Methods that work on a actor.

// ActorRequire is a lazy constructor that locates an actor by identity, or
// returns an error if there is no actor already with that identity.
func (broker *Broker) ActorRequire(receivedFrom []byte) (actor *Actor) {
	actor, ok := broker.actors[string(receivedFrom)]
	if !ok {
		actor = &Actor{
			broker: broker,
			identity: receivedFrom,
			expiry: time.Now().Add(HeartbeatExpiry),
		}
		broker.actors[string(receivedFrom)] = actor
		if broker.verbose {
			log.Printf("I: registering new actor: %q\n", receivedFrom)
		}
	}
	return
}

// DeleteActor deletes the selected actor.
func (broker *Broker) DeleteActor(actor *Actor, disconnect bool) {
	if disconnect {
		disconnectProto, _ := broker.PackageProto(
			mdapi_pb.CommandTypes_DISCONNECT, []string{}, mdapi.Args{})
		actor.Send(disconnectProto, false)
	}
	for serviceName, actors := range broker.actorServiceMap {
		broker.actorServiceMap[serviceName] = delActor(actors, actor)
	}
	broker.removeServices(actor.availableServices)
	delete(broker.actors, string(actor.identity))
}

// Send formats and sends a command to a actor. The caller may also provide a
// command option, and message payload.
func (actor *Actor) Send(
	msgProto *mdapi_pb.WrapperCommand, forward bool) (err error) {
		var msgBytes []byte
		if forward {
			msgBytes, err = proto.Marshal(actor.broker.ForwardProto(msgProto))
		}else {
			msgBytes, err = proto.Marshal(msgProto)
		}
		if err != nil {
			panic(err)
		}
		_, err = actor.broker.socket.SendMessage(
			actor.identity, util.Btou(forward), msgBytes)
		return
}