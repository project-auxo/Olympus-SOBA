package service

import (
	"github.com/Project-Auxo/Olympus/pkg/mdapi"

	// Import services that can be accessed by the loader here.
	"github.com/Project-Auxo/Olympus/pkg/service/a"
	"github.com/Project-Auxo/Olympus/pkg/service/b"
)

// DispatchRequest will load a service from vantage point of the actor.
func DispatchRequest(
	client *mdapi.Mdcli, serviceName string, request []string) {
	checkService(serviceName)

	switch serviceName {
	// Client a does ...
	case "a":
		a.ClientRequest(client, request)
	case "b":
		b.ClientRequest(client, request)
	}
}

// LoadService is used by the coordinator.
func LoadService(worker *mdapi.Mdwrk, serviceName string, request []string) (
	response []string) {
	checkService(serviceName)

	switch serviceName {
	// Actor a does ...
	case "a":
		response = service-a.ActorResponse(worker, request)
	case "b":
		response = b.ActorResponse(worker, request)
	}
	return
}
