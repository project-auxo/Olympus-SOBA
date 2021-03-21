package service

import (
	"log"
	"os"

	"github.com/Project-Auxo/Olympus/pkg/mdapi"
	"github.com/Project-Auxo/Olympus/pkg/util"

	// Import services that can be accessed by the loader here.
	"github.com/Project-Auxo/Olympus/pkg/service/echo"
)

// AvailableServices returns all the available services in the 'service'
// directory.
func AvailableServices() (services []string) {
	files, err := os.ReadDir("./pkg/service")
	if err != nil {
		log.Fatal(err)
	}
	for _, f := range files {
		if f.IsDir() {
			services = append(services, f.Name())
		}
	}
	return
}

func checkService(serviceName string) {
	availableServices := AvailableServices()
	_, found := util.Find(availableServices, serviceName)
	if !found {
		log.Fatalf("Service '%s' is not implemented.", serviceName)
	}
}


// DispatchRequest will load a service from vantage point of the actor.
func DispatchRequest(
	client *mdapi.Mdcli, serviceName string, request []string) {
	checkService(serviceName)

	switch serviceName {
	
	// Client echo does ...
	case "echo":
		echo.ClientRequest(client, request)

	}
}

// LoadService is used by the coordinator.
func LoadService(
	worker *mdapi.Mdwrk, serviceName string, request []string) (response []string){
	checkService(serviceName)

	switch serviceName {

	// Actor echo does ...
	case "echo":
		response = echo.ActorResponse(worker, request)

	}
	return
}
