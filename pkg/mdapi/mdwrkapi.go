package mdapi

import (
	"log"
	"runtime"
	"time"

	"github.com/google/uuid"
	zmq "github.com/pebbe/zmq4"

	"github.com/Project-Auxo/Olympus/pkg/util"
)


const (
	heartbeatLiveness = 3
)


// Mdwrk is the Majordomo Protocol Worker API.
type Mdwrk struct {
	id uuid.UUID
	broker string
	service string
	worker *zmq.Socket	// Socket to broker.
	poller *zmq.Poller
	verbose bool	// Print activity to stdout.

	// Heartbeat management.
	heartbeatAt time.Time	// When to send heartbeat
	liveness int 	// How many attempts left.
	heartbeat time.Duration		// Heartbeat delay, msecs.
	reconnect time.Duration		// Reconnect delay, msecs.
	timestamp time.Time // When the worker was created.

	expectReply bool		// False only at start.
	replyTo string		// Return identity, if any.
}


func (mdwrk *Mdwrk) GetID() uuid.UUID {
	return mdwrk.id
}

// SendToBroker sends a message to the broker.
func (mdwrk *Mdwrk) SendToBroker(
	command string, option string, msg []string) (err error) {
		n := 3
		if option != "" {
			n++
		}
		m := make([]string, n, n+len(msg))
		m = append(m, msg...)

		// Strack protocol envelope to start of message.
		if option != "" {
			m[3] = option
		}
		m[2] = command
		m[1] = MdpWorker
		m[0] = ""

		if mdwrk.verbose {
			log.Printf("I: sending %s to broker %q\n", MdpsCommands[command], m)
		}
		_, err = mdwrk.worker.SendMessage(m)
		return
}

// ConnectToBroker attempts to connect or reconnect to the broker.
func (mdwrk *Mdwrk) ConnectToBroker() (err error) {
	if mdwrk.worker != nil {
		mdwrk.worker.Close()
		mdwrk.worker = nil
	}
	mdwrk.worker, err = zmq.NewSocket(zmq.DEALER)
	err = mdwrk.worker.Connect(mdwrk.broker)
	if mdwrk.verbose {
		log.Printf("I: connecting to broker at %s...\n", mdwrk.broker)
	}
	mdwrk.poller = zmq.NewPoller()
	mdwrk.poller.Add(mdwrk.worker, zmq.POLLIN)

	// Register service with broker.
	err = mdwrk.SendToBroker(MdpReady, mdwrk.service, []string{})

	// If liveness hits zero, queue is considered disconnected.
	mdwrk.liveness = heartbeatLiveness
	mdwrk.heartbeatAt = time.Now().Add(mdwrk.heartbeat)
	
	return
}

// NewMdwrk is a constructor.
func NewMdwrk(
	id uuid.UUID, broker, service string, verbose bool) (mdwrk *Mdwrk, err error) {
	mdwrk = &Mdwrk{
		id: id,
		broker: broker,
		service: service,
		verbose: verbose,
		heartbeat: 2500 * time.Millisecond,
		reconnect: 2500 * time.Millisecond,
		timestamp: time.Now(),
	}
	err = mdwrk.ConnectToBroker()
	runtime.SetFinalizer(mdwrk, (*Mdwrk).Close)
	return
}

// Close is mdwrk's destructor.
func (mdwrk *Mdwrk) Close() {
	if mdwrk.worker != nil {
		mdwrk.worker.Close()
		mdwrk.worker = nil
	}
}

// Tune hearbeat interval and retries to expected network performance.

// SetHeartbeat sets the heartbeat delay.
func (mdwrk *Mdwrk) SetHeartbeat(heartbeat time.Duration) {
	mdwrk.heartbeat = heartbeat
}

// SetReconnect sets the reconnect delay.
func (mdwrk *Mdwrk) SetReconnect(reconnect time.Duration) {
	mdwrk.reconnect = reconnect
}


// Recv sends a reply, if any, to broker and waits for the next request.
func (mdwrk *Mdwrk) Recv(reply []string) (msg []string, err error) {
	// Format and send the reply if we were provided one.
	if len(reply) == 0 && mdwrk.expectReply {
		panic("No reply when expected.")
	}
	if len(reply) > 0 {
		if mdwrk.replyTo == "" {
			panic("mdwrk.replyTo == \"\"")
		}
		m := make([]string, 2, 2+len(reply))
		m = append(m, reply...)
		m[0] = mdwrk.replyTo
		m[1] = ""
		err = mdwrk.SendToBroker(MdpReply, "", m)
	}
	mdwrk.expectReply = true
	
	for {
		var polled []zmq.Polled
		polled, err = mdwrk.poller.Poll(mdwrk.heartbeat)
		if err != nil {
			break 	// Interrupted.
		}

		if len(polled) > 0 {
			msg, err = mdwrk.worker.RecvMessage(0)
			if err != nil {
				break		// Interrupted.
			}
			if mdwrk.verbose {
				log.Printf("I: recevied message from broker: %q\n", msg)
			}
			mdwrk.liveness = heartbeatLiveness

			// Don't try to handle errors, just assert.
			if len(msg) < 3 {
				panic("len(msg) < 3")
			}
			if msg[0] != "" {
				panic("msg[0] != \"\"")
			}
			if msg[1] != MdpWorker {
				panic("msg[1] != MdpwWorker")
			}

			command := msg[2]
			msg = msg[3:]
			switch command {
			case MdpRequest:
				// Pop and save as many addresses as there are up to a nill, for now,
				// save one.
				mdwrk.replyTo, msg = util.Unwrap(msg)
				// Here is where we actually have a message to process; we return it to
				// the caller.
				return 		// We have a request to process.
			case MdpHeartbeat:
				// Do nothing on heartbeats.
			case MdpDisconnect:
				mdwrk.ConnectToBroker()
			default:
				log.Printf("E: invalid input message %q\n", msg)
			}
		} else {
			mdwrk.liveness--
			if mdwrk.liveness == 0 {
				if mdwrk.verbose {
					log.Println("W: disconnected from broker, retrying...")
				}
				time.Sleep(mdwrk.reconnect)
				mdwrk.ConnectToBroker()
			}
		}
		// Send heartbeat if it's time.
		if time.Now().After(mdwrk.heartbeatAt) {
			mdwrk.SendToBroker(MdpHeartbeat, "", []string{})
			mdwrk.heartbeatAt = time.Now().Add(mdwrk.heartbeat)
		}
	}
	return
}