package mdbroker

import (
	"fmt"
	"log"
	"runtime"
	"time"

	zmq "github.com/pebbe/zmq4"

	"github.com/Project-Auxo/Olympus/pkg/util"
	"github.com/Project-Auxo/Olympus/pkg/mdapi"
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


func popStr(ss []string) (s string, ss2 []string) {
	s = ss[0]
	ss2 = ss[1:]
	return
}

func popMsg(msgs [][]string) (msg []string, msgs2 [][]string) {
	msg = msgs[0]
	msgs2 = msgs[1:]
	return
}

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
	actors map[string]*Actor	// Hash of known actors keyed by idString.

	heartbeatAt 	time.Time	// When to send the heartbeat.
}

// Service defines a running service instance and the actors attached to it.
type Service struct {
	broker *Broker // Broker instance.
	name string		// Service name.
	requests [][]string		// List of client requests.
	attachedActors []*Actor 	// List of actors attached to this service.

	timestamp time.Time
}

// Actor class defines a single actor, idle or active.
type Actor struct {
	broker *Broker 	// Broker instance.
	idString string 	// Identity of actor as a string.
	identity string 	// Identity frame for routing.
	runningServices []*Service		// Services that actor is participating in.

	expiry time.Time	// Expires at unless heartbeat.
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

// addServices will add the services that an actor notifies the broker via its
// mdapi.MdpReady and mdapi.MdpHeartbeat
func (broker *Broker) addServices(services []string, actor *Actor) {
	for _, serviceName := range services {
		broker.knownServices[serviceName] = struct{}{}
	}
	// Update the actorServiceMap.
	for serviceName := range broker.knownServices {
		broker.actorServiceMap[serviceName] = append(
			broker.actorServiceMap[serviceName], actor)
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
			msg, err := broker.socket.RecvMessage(0)
			if err != nil {
				break // Interrupted.
			}
			if broker.verbose {
				log.Printf("I: received message: %q\n", msg)
			}
			sender, msg := popStr(msg)
			_, msg = popStr(msg)
			header, msg := popStr(msg)
 
			switch header {
			case mdapi.MdpcClient:
			 broker.ClientMsg(sender, msg)
			case mdapi.MdpActor:
			 broker.ActorMsg(sender, msg)
			default:
			 log.Printf("E: invalid message: %q\n", msg)
			}
		 }
		 // Disconnect and delete any expired actors, send hearbeats to idle actors
		 // if needed.
		 if time.Now().After(broker.heartbeatAt) {
			 broker.Purge()
			 for _, actor := range broker.actors {
				 actor.Send(mdapi.MdpHeartbeat, "", []string{})
			 }
			 broker.heartbeatAt = time.Now().Add(HeartbeatInterval)
		 }
		}
	 log.Println("W: interrupt received, shutting down...")
 }


// ActorMsg processes on MdpwReady, MdpwReply, MdpwHeartbeat or MdpwDisconnect
// message sent to the broker by an actor.
func (broker *Broker) ActorMsg(sender string, msg []string) {
	if len(msg) == 0 {
		panic("len(msg) == 0")
	}

	command, msg := popStr(msg)
	actor := broker.ActorRequire(sender)


	switch command {
	case mdapi.MdpReady:
		// Reserved service name, handle internally.
		if len(sender) >= 4 && sender[:4] == "mmi." {
			log.Println("I: mmi. services have not yet been implemented.")
		} else {
			// Actor notifies broker of its services, update broker's internal state.
			broker.addServices(msg, actor)
		}
	case mdapi.MdpReply:
		serviceName, msg := popStr(msg)
		_, msg = popStr(msg)
		client, msg := util.Unwrap(msg)		// msg is reply body.
		broker.socket.SendMessage(
			client, "", mdapi.MdpcClient, serviceName, msg)
	case mdapi.MdpHeartbeat:
		broker.addServices(msg, actor) // Actor notifies broker of its services.
		actor.expiry = time.Now().Add(HeartbeatExpiry)
	case mdapi.MdpDisconnect:
		actor.Delete(false)
	default:
		log.Printf("E: invalid input message %q\n", msg)
	}
}

// ClientMsg processes a request coming from a client. We implement MMI requests
// directly here (at present, we implement only the mmi.service request).
// TODO: Implement more of the mmi. internal services.
func (broker *Broker) ClientMsg(sender string, msg []string) {
	// Service name + body.
	if len(msg) < 2 {
		panic("len(msg) < 2")
	}

	serviceFrame, msg := popStr(msg)
	service := broker.ServiceRequire(serviceFrame)

	// Set reply return identity to client sender.
	m := []string{sender, ""}
	msg = append(m, msg...)

	// If we got a MMI service request, process that internally.
	if len(serviceFrame) >= 4 && serviceFrame[:4] == "mmi." {
		returnCode := "501"		// Service not implemented.
		msg[len(msg)-1] = returnCode

		// Remove and save client return envelope and insert the protocol header and
		// service name, then rewarap envelope.
		client, msg := util.Unwrap(msg)
		broker.socket.SendMessage(client, "", mdapi.MdpcClient, serviceFrame, msg)
	} else {
		// Else dispatch the message to the requested service.
		service.Dispatch(msg)
	}
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
			log.Println("I: deleting expired actor", actor.idString)
		}
		actor.Delete(false)
	}
}


// Methods that work on a service.

// ServiceRequire is a lazy constructor locates a service by name, or creates a
// new service if there is no service already with that name.
func (broker *Broker) ServiceRequire(serviceFrame string) (service *Service) {
	service, ok := broker.runningServices[serviceFrame]
	if !ok {
		service = &Service{
			broker: broker,
			name: serviceFrame,
			requests: make([][]string, 0),
			attachedActors: make([]*Actor, 0),
			timestamp: time.Now(),
		}
		broker.waitingServices[serviceFrame] = service
		if broker.verbose {
			log.Println("I: added waiting service:", serviceFrame)
		}
	}
	return
}

// Dispatch sends requests to waiting actors.
func (service *Service) Dispatch(msg []string) {
	if len(msg) > 0 {
		// Queue message if any.
		service.requests = append(service.requests, msg)
	}
	service.broker.Purge()
	dispatchableActors := service.broker.actorServiceMap[service.name]

	for len(dispatchableActors) > 0 && len(service.requests) > 0 {
		var actor *Actor
		actor, service.broker.actorServiceMap[service.name] = popActor(
			dispatchableActors)
		
		msg, service.requests = popMsg(service.requests)
		actor.Send(mdapi.MdpRequest, service.name, msg)
	}
}


// Methods that work on a actor.

// ActorRequire is a lazy constructor that locates an actor by identity, or
// returns an error if there is no actor already with that identity.
func (broker *Broker) ActorRequire(identity string) (actor *Actor) {
	idString := fmt.Sprintf("%q", identity)
	actor, ok := broker.actors[idString]
	if !ok {
		actor = &Actor{
			broker: broker,
			idString: idString,
			identity: identity,
			expiry: time.Now().Add(HeartbeatExpiry),
		}
		broker.actors[idString] = actor
		if broker.verbose {
			log.Printf("I: registering new actor: %s\n", idString)
		}
	}
	return
}

// Delete deletes the current actor.
func (actor *Actor) Delete(disconnect bool) {
	if disconnect {
		actor.Send(mdapi.MdpDisconnect, "", []string{})
	}
	for serviceName, actors := range actor.broker.actorServiceMap {
		actor.broker.actorServiceMap[serviceName] = delActor(actors, actor)
	}
	delete(actor.broker.actors, actor.idString)
}

// Send formats and sends a command to a actor. The caller may also provide a
// command option, and message payload.
func (actor *Actor) Send(command, option string, msg []string) (err error) {
	n := 4
	if option != "" {
		n++
	}
	m := make([]string, n, n+len(msg))
	m = append(m, msg...)

	// Stack protocol envelope to start of message.
	if option != "" {
		m[4] = option
	}
	m[3] = command
	m[2] = mdapi.MdpActor

	// Stack routing envelope to start of message.
	m[1] = ""
	m[0] = actor.identity

	if actor.broker.verbose {
		log.Printf("I: sending %s to actor %q\n", mdapi.MdpsCommands[command], m)
	}
	_, err = actor.broker.socket.SendMessage(m)
	return
}