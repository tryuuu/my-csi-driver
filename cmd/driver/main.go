package main

import (
	"log"

	"github.com/tryuuu/my-csi-driver/internal/driver"
)

const defaultSocket = "/csi/csi.sock"

func main() {
	log.Printf("starting %s %s", driver.DriverName, driver.DriverVersion)
	if err := driver.Run(defaultSocket); err != nil {
		log.Fatalf("server exited: %v", err)
	}
}
