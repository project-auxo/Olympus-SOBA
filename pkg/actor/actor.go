package actor


type Actor struct {
	name string
	verbose bool		// Print activity to stdout.
	broker string	
	coordinator  *Coordinator		// Manages workers, e.g. spawning and deletion.
}


// NewActor constructs a new actor.
func NewActor(
	name string,
	loadableServices []string,
	broker string, endpoint string, verbose bool) (actor *Actor) {
	actor = &Actor{
		name: name,
		verbose: verbose,
		broker: broker,
	}
	// Spawn coordinator.
	coordinator, _ := NewCoordinator(broker, endpoint, loadableServices, verbose)
	actor.coordinator = coordinator
	actor.coordinator.ConnectToBroker()
	return
}

// Close destroys the actor -- make sure to cleanly close it's constituing 
// components.
func (actor *Actor) Close() {
	actor.coordinator.Close()
}

// TODO: This function feels like it's in a weird spot.
func (actor *Actor) Run() {
	actor.coordinator.HandleRequests()
}

