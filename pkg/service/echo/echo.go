package echo

import (
	"log"

	"github.com/Project-Auxo/Olympus/pkg/mdapi"
)

// TODO: This should take an echo proto.
/*
Request:
	-

Response:
	-
*/


// ------------ Client --------------------
func ClientRequest(client *mdapi.Mdcli, request []string) {
	var count int
	numTries := 1
	for count = 0; count < numTries; count++ {
		err := client.Send("echo", "Hello World!")	// FIXME: Replace "Hello World"
		if err != nil {
			log.Println("Send:", err)
			break
		}
	}
	for count = 0; count < numTries; count++ {
		_, err := client.Recv()
		if err != nil {
			break
		}
	}
	log.Printf("%d replies received\n", count)
}


// ------------- Actor -------------------
// Recall that the worker is controlled by the actor's internal coordinator,
// i.e. worker's response is handed-off to its respective coordinator.
func ActorResponse(worker *mdapi.Mdwrk, request []string) (reply []string) {
	return request		// Echo is not complicated.
}