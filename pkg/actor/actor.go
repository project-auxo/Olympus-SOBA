package actor

import (
	"log"
	"time"
)

type Actor struct {
	name string
	verbose bool		// Print activity to stdout.
	broker string	
	coordinator  *Coordinator		// Manages workers, e.g. spawning and deletion.
}


// NewActor constructs a new actor.
func NewActor(name string, broker string, verbose bool) (actor *Actor) {
	actor = &Actor{
		name: name,
		verbose: verbose,
		broker: broker,
	}
	// Spawn coordinator.
	coordinator := &Coordinator{
		broker: broker,
		verbose: verbose,
		heartbeat: 2500 * time.Millisecond,
		reconnect: 2500 * time.Millisecond,
	}
	actor.coordinator = coordinator
	actor.coordinator.ConnectToBroker()
	// runtime.SetFinalizer(actor, (*Actor).Close)

	if verbose {
		log.Println("Initialized new actor", name)
	}
	return
}

// Close destroys the actor -- make sure to cleanly close it's constituing 
// components.
func (actor *Actor) Close() {
	if actor.coordinator != nil {
		actor.coordinator.brokerSocket.Close()
		actor.coordinator.brokerSocket = nil
	}
}

// RunService runs a service of a particular name.
func (actor *Actor) RunService(serviceName string) {
	var err error
	var request, reply []string
	for {
		request, err = actor.coordinator.RecvFromBroker(reply)
		if err != nil {
			break 		// Actor was interrupted.
		}
		reply = actor.coordinator.Work(serviceName, request)
	}
	log.Println(err)
}

