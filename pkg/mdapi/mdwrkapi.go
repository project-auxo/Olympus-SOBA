package mdapi

import (
	"log"
	"runtime"

	"github.com/google/uuid"
	zmq "github.com/pebbe/zmq4"
)

// TODO: Replace all "mdwrk" with "worker" to get consistency across codebase.

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
	service string,
	coordinator string, verbose bool, identity string) (mdwrk *Mdwrk, err error) {
	mdwrk = &Mdwrk{
	id: id,
	service: service,
	coordinator: coordinator,
	verbose: verbose,
	}
	err = mdwrk.ConnectToCoordinator(identity)
	mdwrk.SendToCoordinator(MdpReady, "", []string{})
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
		log.Printf("worker: connecting to coordinator at %s...\n", mdwrk.coordinator)
	}
	return
}


// SendToCoordinator sends a message to the coordinator.
func (mdwrk *Mdwrk) SendToCoordinator(
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
			log.Printf(
				"worker: sending %s to coordinator %q\n", MdpsCommands[command], m)
		}
		_, err = mdwrk.coordinatorSocket.SendMessage(m)
		return
}


func (mdwrk *Mdwrk) RecvFromCoordinator() (request []string) {
	msg, _ := mdwrk.coordinatorSocket.RecvMessage(0)
	command := msg[2]

	switch command {
	case MdpRequest:
		msg = append(msg, mdwrk.id.String())
		return msg[6:]
	case MdpDisconnect:
		if mdwrk.verbose {
			log.Printf("Worker %s killing self.", mdwrk.id.String())
		}
		mdwrk.Close()
	}
	return []string{""}
}