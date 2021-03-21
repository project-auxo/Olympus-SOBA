package main

import (
	"fmt"
	"flag"

	"github.com/Project-Auxo/Olympus/pkg/mdbroker"
	"github.com/Project-Auxo/Olympus/config"
)

// TODO: Have panic somewhere to do with config errors.
// TODO: Transition to using protos for the messages.

func main() {
	verbosePtr := flag.Bool("v", false, "Print to stdout.")
	configPathPtr := flag.String("config", "", "Absolute path to config file.")
	flag.Parse()
	
	verbose := *verbosePtr
	configPath := *configPathPtr

	configuration, _ := config.LoadConfig(configPath)

	endpoint := fmt.Sprintf(
		"tcp://%s:%d", configuration.Broker.Hostname, configuration.Broker.Port)

	broker, _ := mdbroker.NewBroker(verbose)
	broker.Bind(endpoint)
	broker.Handle()
}