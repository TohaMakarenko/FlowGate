package main

import (
	"bytes"
	"context"
	"github.com/TohaMakarenko/FlowGate/shared"
	"log"
	"net/http"
)

type HttpMessageDispatcher struct {
	msgChanel  chan *shared.Message
	configRepo IDispatchingConfigRepository
}

func NewHttpMessageDispatcher(configRepo IDispatchingConfigRepository) *HttpMessageDispatcher {
	dispatcher := HttpMessageDispatcher{
		configRepo: configRepo,
	}
	return &dispatcher
}

func (dispatcher *HttpMessageDispatcher) Start(ctx context.Context, msgChanel chan *shared.Message) {
	dispatcher.msgChanel = msgChanel
	for message := range msgChanel {
		config, ok := dispatcher.configRepo.Get(message.EventType)
		if !ok {
			log.Printf("config for %v not found", message.EventType)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, config.targetEndpoint, bytes.NewReader(message.Body))
		if err != nil {
			log.Printf("failed creating request for message %v. Error: %v", message.EventType, err)
		}

		_, err = http.DefaultClient.Do(req)

		if err != nil {
			log.Printf("failed sending http message %v. Error: %v", message.EventType, err)
		} else {
			log.Printf("message %v dispatched", message.EventType)
		}
	}
}
