package main

import (
	"fmt"
	"log"
	"flag"

	"github.com/Project-Auxo/Olympus/pkg/mdapi"
	mdapi_pb "github.com/Project-Auxo/Olympus/proto/mdapi"

	"github.com/Project-Auxo/Olympus/config"
	// "github.com/Project-Auxo/Olympus/pkg/service"
)


func main() {
	idPtr := flag.Int("name", 1, "Client's ID.")
	verbosePtr := flag.Bool("v", false, "Print to stdout.")
	configPathPtr := flag.String("config", "", "Absolute path to config file.")
	serviceNamePtr := flag.String("s", "", "Service name to request")
	flag.Parse()
	
	id := *idPtr
	verbose := *verbosePtr
	configPath := *configPathPtr
	serviceName := *serviceNamePtr

	configuration, err := config.LoadConfig(configPath)
	if err != nil {
		log.Fatal(err)
	}
	
	brokerHostname := configuration.Broker.Hostname
	if brokerHostname == "*" {
		brokerHostname = "localhost"
	}

	brokerEndpoint := fmt.Sprintf(
		"tcp://%s:%d", brokerHostname, configuration.Broker.Port)
	client, _ := mdapi.NewMdcli(id, brokerEndpoint, verbose)

	// TODO: Fix the service, loader issues, so as to use correct dispatch.
	// service.DispatchRequest(client, serviceName, []string{})
	var count int
	numTries := 1
	for count = 0; count < numTries; count++ {
		requestProto, _ := client.PackageProto(mdapi_pb.CommandTypes_REQUEST,
			[]string{"Hello World"}, mdapi.Args{ServiceName: serviceName})
		err := client.SendToBroker(requestProto)
		if err != nil {
			log.Println("Send:", err)
			break
		}
	}
	for count = 0; count < numTries; count++ {
		recvProto := client.RecvFromBroker()
		log.Println(recvProto)
	}
	log.Printf("%d replies received\n", count)
}