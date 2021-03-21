package mdapi

import (
	"errors"
	"log"
	"runtime"
	"time"

	zmq "github.com/pebbe/zmq4"
)

var	errPermanent = errors.New("permanent error, abandoning request")


// Mdcli is the Majordomo Protocol Client API.
type Mdcli struct {
	ID int
	broker string
	client *zmq.Socket		// Socket to the broker.
	verbose bool		// Print activity to stdout.
	timeout time.Duration 	// Request timeout.
	poller *zmq.Poller
}


// ConnectToBroker to connect or reconnect to the broker. Asynchronous hence
// the DEALER socket over the REQ socket.
func (mdcli *Mdcli) ConnectToBroker() (err error) {
	if mdcli.client != nil {
		mdcli.client.Close()
		mdcli.client = nil
	}
	mdcli.client, err = zmq.NewSocket(zmq.DEALER)
	if err != nil {
		if mdcli.verbose {
			log.Println("E: ConnectToBroker() creating socket failed")
		}
		return
	}

	mdcli.poller = zmq.NewPoller()
	mdcli.poller.Add(mdcli.client, zmq.POLLIN)

	if mdcli.verbose {
		log.Printf("I: connecting to broker at %s...", mdcli.broker)
	}
	err = mdcli.client.Connect(mdcli.broker)
	if err != nil && mdcli.verbose {
		log.Println(
			"E: ConnectToBroker() failed to connect to broker", mdcli.broker)

	}

	return
}


// NewMdcli is a constructor.
func NewMdcli(id int, broker string, verbose bool) (mdcli *Mdcli, err error) {
	mdcli = &Mdcli{
		ID: id,
		broker: broker,
		verbose: verbose,
		timeout: time.Duration(2500*time.Millisecond),
	}
	err = mdcli.ConnectToBroker()
	runtime.SetFinalizer(mdcli, (*Mdcli).Close)
	return
}


// Close is mdcli's destructor.
func (mdcli *Mdcli) Close() (err error) {
	if mdcli.client != nil {
		err = mdcli.client.Close()
		mdcli.client = nil
	}
	return
}

// SetTimeout sets the request timeout.
func (mdcli *Mdcli) SetTimeout(timeout time.Duration) {
	mdcli.timeout = timeout
}

// Send sends just one message without waiting for reply. Using DEALER socket so
// we send an empty frame at the start.
func (mdcli *Mdcli) Send(service string, request ...string) (err error) {
	// Prefix request with protocol frames.
	// Frame 0: empty (REQ emulation).
	// FRAME 1: "MDPCxy" (six bytes, MDP/Client x.y).
	// FRAME 2: Service name (printable string)

	req := make([]string, 3, len(request)+3)
	req = append(req, request...)
	req[2] = service
	req[1] = MdpcClient
	req[0] = ""
	if mdcli.verbose {
		log.Printf("I: send request to '%s' service: %q\n", service, request)
	}
	_, err = mdcli.client.SendMessage(req)
	return
}

// Recv waits for a reply message and returns that to the caller. Returns the
// replay message or Nil if there was no reply. Does not attempt to recover from
// a broker failure, this is not possible without storing all unanswered
// requests and resending them all.
func (mdcli *Mdcli) Recv() (msg []string, err error) {
	msg = []string{}

	// Poll socket for a reply with timeout.
	polled, err := mdcli.poller.Poll(mdcli.timeout)
	if err != nil {
		return 
	}

	// If we got a reply, process it.
	if len(polled) > 0 {
		msg, err = mdcli.client.RecvMessage(0)
		if err != nil {
			log.Println("W: interrupt received, killing client...")
			return
		}

		if mdcli.verbose {
			log.Printf("I: received reply: %q\n", msg)
		}

		if len(msg) < 4 {
			panic("len(msg) < 4")
		}
		if msg[0] != "" {
			panic("msg[0]  != \"\"")
		}
		if msg[1] != MdpcClient {
			panic("msg[1] != MdpcClient")
		}

		msg = msg[3:]
		return
	}

	err = errPermanent
	if mdcli.verbose {
		log.Println(err)
	}
	return
}