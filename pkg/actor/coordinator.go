package actor

import (
	"log"
	"time"

	zmq "github.com/pebbe/zmq4"

	"github.com/Project-Auxo/Olympus/pkg/mdapi"
	"github.com/Project-Auxo/Olympus/pkg/service"
	"github.com/Project-Auxo/Olympus/pkg/util"
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
	workerSocket *zmq.Socket 
	brokerSocket *zmq.Socket
	broker string 	// Coordinator binds to this endpoint.
	poller *zmq.Poller

	verbose bool 	// Print activity to stdout
	// services map[string]*mdapi.Mdwrk		// Hash of current running services.

	service string

	// Heartbeat management.
	heartbeatAt time.Time // When to send heartbeat.
	liveness int	// How many attempts left.
	heartbeat time.Duration 	// Heartbeat delay, msecs.
	reconnect time.Duration // Reconnect delay, msecs.

	expectReply  bool 		// False only at start.
	replyTo	string		// Return identity, if any.
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
	coordinator.brokerSocket, err = zmq.NewSocket(zmq.DEALER)
	err = coordinator.brokerSocket.Connect(coordinator.broker)
	if coordinator.verbose {
		log.Printf("I: connecting to broker at %s...\n", coordinator.broker)
	}
	coordinator.poller = zmq.NewPoller()
	coordinator.poller.Add(coordinator.brokerSocket, zmq.POLLIN)

	// Register coordinator with the broker.
	err = coordinator.SendToBroker(
		mdapi.MdpReady, coordinator.service, []string{})
	
	// If liveness hits zero, queue is considered disconnected.
	coordinator.liveness = heartbeatLiveness
	coordinator.heartbeatAt = time.Now().Add(coordinator.heartbeat)

	return
}

// RecvFromBroker sends a reply, if any to broker and waits for the next
// request.
func (coordinator *Coordinator) RecvFromBroker(
	reply []string) (msg []string, err error) {
	// Format and send the reply if we were provided one.
	if len(reply) == 0 && coordinator.expectReply {
		panic("No reply when expected.")
	}
	if len(reply) > 0 {
		if coordinator.replyTo == "" {
			panic("coordinator.replyTo == \"\"")
		}
		m := make([]string, 2, 2+len(reply))
		m = append(m, reply...)
		m[0] = coordinator.replyTo
		m[1] = ""
		_ = coordinator.SendToBroker(mdapi.MdpReply, "", m)
	}
	coordinator.expectReply = true 

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
				// Pop and save as many addresses as there are up to a nil, for now save
				// one.
				coordinator.replyTo, msg = util.Unwrap(msg)
				// Here is where we actually have a message to process; we return it to
				// the caller.
				return		// We have a request to process
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
			coordinator.SendToBroker(mdapi.MdpHeartbeat, "", []string{})
			coordinator.heartbeatAt = time.Now().Add(coordinator.heartbeat)
		}
	}
	return
}


// ----------------- Worker Interface ------------------------

// Binds will bind the coordinator instance to an endpoint. We use single socket
// for connection to workers and then to broker.
// func (coordinator *Coordinator) Bind(endpoint string) (err error) {
// 	err = coordinator.socket.Bind(endpoint)
// 	if err != nil {
// 		log.Printf(
// 			"E: coordinator failed to bind at %s", endpoint)
// 	}
// 	log.Printf("I: Coordinator is active at %s", endpoint)
// 	return
// }

// Work has the coordinator spawn and use a worker to respond to a client
// request.
func (coordinator *Coordinator) Work(
	serviceName string, request []string) (response []string) {
	x := mdapi.Mdwrk{}
	return service.LoadService(&x, serviceName, request)
}

// Spawn has the coordinator search through and spawn a worker to answer
// a service request.
func (coordinator *Coordinator) Spawn(service string) {
	// When spawning a worker, worker connects to the coordinator via inproc
	// transport.
}

