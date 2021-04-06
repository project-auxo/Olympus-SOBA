package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/Project-Auxo/Olympus/config"
	agent "github.com/Project-Auxo/Olympus/pkg/agent"
	service "github.com/Project-Auxo/Olympus/pkg/service"
)

func init() {
	// Workers use this service loader.
	agent.LoadService = service.LoadService
}

func main() {
	actorNamePtr := flag.String("name", "A01", "Actor's name.")
	verbosePtr := flag.Bool("v", false, "Print to stdout.")
	configPathPtr := flag.String("config", "", "Absolute path to config file.")
	flag.Parse()

	actorName := *actorNamePtr
	verbose := *verbosePtr
	configPath := *configPathPtr

	configuration, err := config.LoadConfig(configPath)
	if err != nil {
		log.Fatal(err)
	}
	loadableServices := configuration.Actor.Services

	brokerHostname := configuration.Broker.Hostname
	if brokerHostname == "*" {
		brokerHostname = "localhost"
	}
	broker := fmt.Sprintf(
		"tcp://%s:%d", brokerHostname, configuration.Broker.Port)

	actorHostname := configuration.Actor.Hostname
	actorEndpoint := actorHostname
	if !strings.Contains(actorHostname, "inproc") {
		if actorHostname == "*" {
			actorHostname = "localhost"
		}
		actorEndpoint = fmt.Sprintf(
			"tcp://%s:%d", actorHostname, configuration.Actor.Port)
	}

	actor := agent.NewActor(
		actorName, loadableServices, broker, actorEndpoint, verbose)
	actor.Run()
}
