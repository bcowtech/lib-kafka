package kafka

import "log"

type ForwarderRunner struct {
	handle *Forwarder
}

func (r *ForwarderRunner) Start() {
	log.Println("Started")
}

func (r *ForwarderRunner) Stop() {
	log.Println("Stopping")
	r.handle.Close()
	log.Println("Stopped")
}
