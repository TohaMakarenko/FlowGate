package main

type DispatchingConfig struct {
	eventType      string
	targetEndpoint string
	staticParams   map[string]string
}
