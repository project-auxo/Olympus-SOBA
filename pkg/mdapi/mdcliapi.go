package mdapi

import (
	"errors"
	"log"
	"runtime"
	"time"

	zmq "github.com/pebbe/zmq4"
	"google.golang.org/protobuf/proto"

	mdapi_pb "github.com/Project-Auxo/Olympus/proto/mdapi"
)

var	errPermanent = errors.New("permanent error, abandoning request")
// TODO: Change client to clientSocket

// Mdcli is the Majordomo Protocol Client API.
type Mdcli struct {
	identity string
	broker string
	client *zmq.Socket		// Socket to the broker.
	verbose bool		// Print activity to stdout.
	timeout time.Duration 	// Request timeout.
	poller *zmq.Poller
}


// ConnectToBroker to connect or reconnect to the broker. Asynchronous hence
// the DEALER socket over the REQ socket.
func (mdcli *Mdcli) ConnectToBroker(identity string) (err error) {
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

	if identity != "" {
		mdcli.client.SetIdentity(identity)
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
func NewMdcli(identity string, broker string, verbose bool) (mdcli *Mdcli, err error) {
	mdcli = &Mdcli{
		identity: identity,
		broker: broker,
		verbose: verbose,
		timeout: time.Duration(2500*time.Millisecond),
	}
	err = mdcli.ConnectToBroker(identity)
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


// PackageProto will marshal the given information into the correct bytes
// package.
func (mdcli *Mdcli) PackageProto(
	commandType mdapi_pb.CommandTypes, msg []string,
	args Args) (msgProto *mdapi_pb.WrapperCommand, err error) {
		msgProto = &mdapi_pb.WrapperCommand{
			Header: &mdapi_pb.Header{
				Type: commandType,
				Entity: mdapi_pb.Entities_CLIENT,
				Origin: mdcli.identity,
				Address: mdcli.identity, 
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
		default:
			log.Fatalf("E-C: uknown commandType %q", commandType)
		}
		return
}

func (mdcli *Mdcli) SendToBroker(
	msgProto *mdapi_pb.WrapperCommand) (err error) {
		commandType := msgProto.GetHeader().GetType()
		msgBytes, err := proto.Marshal(msgProto)
		if err != nil {
			panic(err)
		}
	
		if mdcli.verbose {
			log.Printf("I: send %s to coordinator\n", CommandMap[commandType])
		}
		// First part is the "forwarded" bit, set to 0 because clients can't forward
		// messages.
		_, err = mdcli.client.SendMessage(0, msgBytes)
		return
}


// Recv waits for a reply message and returns that to the caller. Returns the
// replay message or Nil if there was no reply. Does not attempt to recover from
// a broker failure, this is not possible without storing all unanswered
// requests and resending them all.
func (mdcli *Mdcli) RecvFromBroker() (msgProto *mdapi_pb.WrapperCommand) {
	// Poll socket for a reply with timeout.
	polled, err := mdcli.poller.Poll(mdcli.timeout)
	if err != nil {
		return 
	}

	// If we got a reply, process it.
	if len(polled) > 0 {
		recvBytes, err := mdcli.client.RecvMessageBytes(0)
		if err != nil {
			log.Println("E: interrupt received, killing client...")
			return
		}
		msgProto = &mdapi_pb.WrapperCommand{}
		if err := proto.Unmarshal(recvBytes[0], msgProto); err != nil {
			log.Fatalln("E: failed to parse wrapped command", err)
		}

		if mdcli.verbose {
			log.Printf("I: received reply: %q\n", msgProto.GetRequest().GetBody())
		}

		// TODO: Insert the assertions on the proto.
		return
	}

	err = errPermanent
	if mdcli.verbose {
		log.Println(err)
	}
	return
}