package main

import (
	"fmt"
	"log"
	"flag"

	"github.com/Project-Auxo/Olympus/pkg/mdapi"
	"github.com/Project-Auxo/Olympus/config"
	"github.com/Project-Auxo/Olympus/pkg/service"
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

	service.DispatchRequest(client, serviceName, []string{})
}