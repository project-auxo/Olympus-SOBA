package agent

import (
	"errors"
	"log"
	"runtime"
	"time"

	zmq "github.com/pebbe/zmq4"
	"google.golang.org/protobuf/proto"

	mdapi "github.com/Project-Auxo/Olympus/pkg/mdapi"
	mdapi_pb "github.com/Project-Auxo/Olympus/proto/mdapi"
)

var	errPermanent = errors.New("permanent error, abandoning request")
// TODO: Change clientSocket to clientSocket

// Client is the Majordomo Protocol Client API.
type Client struct {
	identity string
	broker string
	clientSocket *zmq.Socket		// Socket to the broker.
	verbose bool		// Print activity to stdout.
	timeout time.Duration 	// Request timeout.
	poller *zmq.Poller
}


// ConnectToBroker to connect or reconnect to the broker. Asynchronous hence
// the DEALER socket over the REQ socket.
func (client *Client) ConnectToBroker(identity string) (err error) {
	if client.clientSocket != nil {
		client.clientSocket.Close()
		client.clientSocket = nil
	}
	client.clientSocket, err = zmq.NewSocket(zmq.DEALER)
	if err != nil {
		if client.verbose {
			log.Println("E: ConnectToBroker() creating socket failed")
		}
		return
	}

	if identity != "" {
		client.clientSocket.SetIdentity(identity)
	}

	client.poller = zmq.NewPoller()
	client.poller.Add(client.clientSocket, zmq.POLLIN)

	if client.verbose {
		log.Printf("I: connecting to broker at %s...", client.broker)
	}
	err = client.clientSocket.Connect(client.broker)
	if err != nil && client.verbose {
		log.Println(
			"E: ConnectToBroker() failed to connect to broker", client.broker)
	}
	return
}


// NewClient is a constructor.
func NewClient(identity string, broker string, verbose bool) (client *Client, err error) {
	client = &Client{
		identity: identity,
		broker: broker,
		verbose: verbose,
		timeout: time.Duration(2500*time.Millisecond),
	}
	err = client.ConnectToBroker(identity)
	runtime.SetFinalizer(client, (*Client).Close)
	return
}


// Close is client's destructor.
func (client *Client) Close() (err error) {
	if client.clientSocket != nil {
		err = client.clientSocket.Close()
		client.clientSocket = nil
	}
	return
}

// SetTimeout sets the request timeout.
func (client *Client) SetTimeout(timeout time.Duration) {
	client.timeout = timeout
}


// PackageProto will marshal the given information into the correct bytes
// package.
func (client *Client) PackageProto(
	commandType mdapi_pb.CommandTypes, msg []string,
	args Args) (msgProto *mdapi_pb.WrapperCommand, err error) {
		msgProto = &mdapi_pb.WrapperCommand{
			Header: &mdapi_pb.Header{
				Type: commandType,
				Entity: mdapi_pb.Entities_CLIENT,
				Origin: client.identity,
				Address: client.identity, 
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

func (client *Client) SendToBroker(
	msgProto *mdapi_pb.WrapperCommand) (err error) {
		commandType := msgProto.GetHeader().GetType()
		msgBytes, err := proto.Marshal(msgProto)
		if err != nil {
			panic(err)
		}
	
		if client.verbose {
			log.Printf("I: send %s to coordinator\n", mdapi.CommandMap[commandType])
		}
		// First part is the "forwarded" bit, set to 0 because clients can't forward
		// messages.
		_, err = client.clientSocket.SendMessage(0, msgBytes)
		return
}


// Recv waits for a reply message and returns that to the caller. Returns the
// replay message or Nil if there was no reply. Does not attempt to recover from
// a broker failure, this is not possible without storing all unanswered
// requests and resending them all.
func (client *Client) RecvFromBroker() (msgProto *mdapi_pb.WrapperCommand) {
	// Poll socket for a reply with timeout.
	polled, err := client.poller.Poll(client.timeout)
	if err != nil {
		return 
	}

	// If we got a reply, process it.
	if len(polled) > 0 {
		recvBytes, err := client.clientSocket.RecvMessageBytes(0)
		if err != nil {
			log.Println("E: interrupt received, killing client...")
			return
		}
		msgProto = &mdapi_pb.WrapperCommand{}
		if err := proto.Unmarshal(recvBytes[0], msgProto); err != nil {
			log.Fatalln("E: failed to parse wrapped command", err)
		}

		if client.verbose {
			log.Printf("I: received reply: %q\n", msgProto.GetRequest().GetBody())
		}

		// TODO: Insert the assertions on the proto.
		return
	}

	err = errPermanent
	if client.verbose {
		log.Println(err)
	}
	return
}