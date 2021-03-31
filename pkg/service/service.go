package service

import (
	"os"
	"log"

	"github.com/Project-Auxo/Olympus/pkg/util"
)

// AvailableServices returns all the available services in the 'service'
// directory.
func AvailableServices(serviceFolderPath string) (services []string) {
	if serviceFolderPath == "" {
		serviceFolderPath = "./pkg/service"
	}
	files, err := os.ReadDir(serviceFolderPath)
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
	availableServices := AvailableServices("")
	_, found := util.Find(availableServices, serviceName)
	if !found {
		log.Fatalf("Service '%s' is not implemented.", serviceName)
	}
}