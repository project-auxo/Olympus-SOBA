package main

import (
	"fmt"
	"log"
	"flag"

	agent "github.com/Project-Auxo/Olympus/pkg/agent"
	mdapi_pb "github.com/Project-Auxo/Olympus/proto/mdapi"
	service "github.com/Project-Auxo/Olympus/pkg/service"

	"github.com/Project-Auxo/Olympus/config"
	// "github.com/Project-Auxo/Olympus/pkg/service"
)


func main() {
	idPtr := flag.String("name", "client-1", "Client's ID.")
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
	client, _ := agent.NewClient(id, brokerEndpoint, verbose)

	requestProto, _ := client.PackageProto(mdapi_pb.CommandTypes_REQUEST,
		[]string{"Hello World!"}, agent.Args{ServiceName: serviceName})
	service.DispatchRequest(client, requestProto)
}