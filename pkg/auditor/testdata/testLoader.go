package service

import (
	"github.com/Project-Auxo/Olympus/pkg/mdapi"

	// Import services that can be accessed by the loader here.
	"github.com/Project-Auxo/Olympus/pkg/service/service-a"
	"github.com/Project-Auxo/Olympus/pkg/service/service-b"

	// [Only valid for "MISSING" test case]: file fails to import service-c when
	// it has been found as a service directory by *.AvailableServices("").
	// "github.com/Project-Auxo/Olympus/pkg/service/service-c"
)

// DispatchRequest will load a service from vantage point of the actor.
func DispatchRequest(
	client *mdapi.Mdcli, serviceName string, request []string) {
	checkService(serviceName)

	switch serviceName {
	// Client a does ...
	case "service-a":
		a.ClientRequest(client, request)
	}
}

// LoadService is used by the coordinator.
func LoadService(worker *mdapi.Mdwrk, serviceName string, request []string) (
	response []string) {
	checkService(serviceName)

	switch serviceName {
	// Actor a does ...
	case "service-a":
		response = a.ActorResponse(worker, request)
	}
	return
}
