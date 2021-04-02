package actor

import (
	"log"
	"time"

	// "github.com/google/uuid"
	zmq "github.com/pebbe/zmq4"

	"github.com/Project-Auxo/Olympus/pkg/mdapi"
	"github.com/Project-Auxo/Olympus/pkg/util"
	// "github.com/Project-Auxo/Olympus/pkg/service"
	// "github.com/Project-Auxo/Olympus/pkg/util"
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

type Coordinator struct {
	// workerSocket *zmq.Socket 
	brokerSocket *zmq.Socket		// Interface with the broker.
	workerSocket *zmq.Socket		// Interface with the internal workers.
	broker string 	// Coordinator connects to broker through this endpoint.
	endpoint string // Coordinator binds to this endpoint.
	poller *zmq.Poller

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
	broker string,
	endpoint string,
	loadableServices []string,
	verbose bool) (coordinator *Coordinator, err error) {
	// Initialize broker state.
	coordinator = &Coordinator{
		broker: broker,
		endpoint: endpoint,
		loadableServices: loadableServices,
		heartbeat: 2500 * time.Millisecond,
		reconnect: 2500 * time.Millisecond,
		verbose: verbose,
	}
	coordinator.workerSocket, err = zmq.NewSocket(zmq.ROUTER)
	return
}


// Binds will bind the coordinator instance to an endpoint. Use 
func (coordinator *Coordinator) Bind(endpoint string) (err error) {
	err = coordinator.workerSocket.Bind(endpoint)
	if err != nil {
		log.Printf(
			"E: coordinator failed to bind at %s", endpoint)
	}
	log.Printf("I: Coordinator is active at %s", endpoint)
	return
}

func (coordinator *Coordinator) Close() {
	if coordinator.brokerSocket != nil {
		coordinator.brokerSocket.Close()
		coordinator.brokerSocket = nil
	}
}

// HandleRequests will handle various requests.
func (coordinator *Coordinator) HandleRequests() {
	var reply []string
	for {
		msg, _ := coordinator.RecvFromBroker(reply)
		/* msg is of the form
			Frame 0: Service name
			Frame 1: Sender identity
			Frame 2: Empty
			Frame 3+: Request payload
		*/
		serviceName, msg := util.PopStr(msg)
		senderIdentity, payload := util.Unwrap(msg)
		replyPayload := coordinator.Work(serviceName, senderIdentity, payload)
	
		reply = make([]string, 4, 4+len(replyPayload))
		reply = append(reply, replyPayload...)
		reply[3] = ""
		reply[2] = senderIdentity
		reply[1] = ""
		reply[0] = serviceName
	}
}


// ----------------- Broker Interface ------------------------

// SendToBroker sends a message to the broker.
func (coordinator *Coordinator) SendToBroker(
	command string, option string, msg []string) (err error) {
		n := 3
		if option != "" {
			n++
		}
		m := make([]string, n, n+len(msg))
		m = append(m, msg...)

		// Stack protocol envelope to start of message.
		if option != "" {
			m[3] = option
		}
		m[2] = command
		m[1] = mdapi.MdpActor
		m[0] = ""

		if coordinator.verbose {
			log.Printf("I: sending %s to broker %q\n", mdapi.MdpsCommands[command], m)
		}
		_, err = coordinator.brokerSocket.SendMessage(m)
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
		log.Printf("I: connecting to broker at %s...\n", coordinator.broker)
	}
	coordinator.poller = zmq.NewPoller()
	coordinator.poller.Add(coordinator.brokerSocket, zmq.POLLIN)

	// Register coordinator with the broker.
	err = coordinator.SendToBroker(
		mdapi.MdpReady, "", coordinator.loadableServices)
	
	// If liveness hits zero, queue is considered disconnected.
	coordinator.liveness = heartbeatLiveness
	coordinator.heartbeatAt = time.Now().Add(coordinator.heartbeat)

	return
}

// RecvFromBroker first sends a reply, if any to broker and waits for the next
// request.
func (coordinator *Coordinator) RecvFromBroker(
	reply []string) (msg []string, err error) {
	// Format and send the reply if we were provided one.
	if len(reply) > 0 {
		replyTo := reply[2]
		if replyTo == "" {
			panic("replyTo == \"\"")
		}
		_ = coordinator.SendToBroker(mdapi.MdpReply, "", reply)
	}

	// Received next request or other command.
	for {
		var polled []zmq.Polled
		polled, err = coordinator.poller.Poll(coordinator.heartbeat)
		if err != nil {
			break		// Interrupted. 
		}

		if len(polled) > 0 {
			msg, err = coordinator.brokerSocket.RecvMessage(0)
			if err != nil {
				break 	// Interrupted.
			}
			if coordinator.verbose {
				log.Printf("I: received message from broker: %q\n", msg)
			}
			coordinator.liveness = heartbeatLiveness

			// Don't try to handle errors, just assert.
			if len(msg) < 3 {
				panic("len(msg) < 3")
			}
			if msg[0] != "" {
				panic("msg[0] != \"\"")
			}
			if msg[1] != mdapi.MdpActor {
				panic("msg[1] != MdpActor")
			}

			command := msg[2]
			msg = msg[3:]
			switch command {
			case mdapi.MdpRequest:
				return		// We have a request to process.
			case mdapi.MdpHeartbeat:
				// Do nothing on heartbeats.
			case mdapi.MdpDisconnect:
				coordinator.ConnectToBroker()
			default:
				log.Printf("E: invalid input message %q\n", msg)
			}
		} else {
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
			coordinator.SendToBroker(
				mdapi.MdpHeartbeat, "", coordinator.loadableServices)
			coordinator.heartbeatAt = time.Now().Add(coordinator.heartbeat)
		}
	}
	return
}


// ----------------- Worker Interface ------------------------

// Work has the coordinator spawn and use a worker to respond to a client
// request.
func (coordinator *Coordinator) Work(serviceName string, replyTo string,
	requestPayload []string) (response []string) {
	// id := uuid.New()
	// worker, _ := mdapi.NewMdwrk(id, serviceName, "inproc://workers", true, "worker1")
	// return service.LoadService(worker, serviceName, request)
	return requestPayload
}

func (coordinator *Coordinator) SendToWorker(identity string, msg []string) {

}


