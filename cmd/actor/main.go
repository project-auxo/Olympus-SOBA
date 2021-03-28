package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/Project-Auxo/Olympus/config"
	act "github.com/Project-Auxo/Olympus/pkg/actor"
)

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
	loadableServices := config.GetActorServices(configPath)

	brokerHostname := configuration.Broker.Hostname
	if brokerHostname == "*" {
		brokerHostname = "localhost"
	}

	brokerEndpoint := fmt.Sprintf(
		"tcp://%s:%d", brokerHostname, configuration.Broker.Port)
	actor := act.NewActor(actorName, loadableServices, brokerEndpoint, verbose)
	actor.RunService("echo")
}
