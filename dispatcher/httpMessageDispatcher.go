package main

import (
	"bytes"
	"context"
	"log"
	"net/http"
)

type HttpMessageDispatcher struct {
	msgChanel  chan *Message
	configRepo IDispatchingConfigRepository
}

func NewHttpMessageDispatcher(configRepo IDispatchingConfigRepository) *HttpMessageDispatcher {
	dispatcher := HttpMessageDispatcher{
		configRepo: configRepo,
	}
	return &dispatcher
}

func (dispatcher *HttpMessageDispatcher) Start(ctx context.Context, msgChanel chan *Message) {
	dispatcher.msgChanel = msgChanel
	for message := range msgChanel {
		config, ok := dispatcher.configRepo.Get(message.eventType)
		if !ok {
			log.Printf("config for %v not found", message.eventType)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, config.targetEndpoint, bytes.NewReader(message.body))
		if err != nil {
			log.Printf("failed creating request for message %v. Error: %v", message.eventType, err)
		}

		_, err = http.DefaultClient.Do(req)

		if err != nil {
			log.Printf("failed dispatching message %v. Error: %v", message.eventType, err)
		} else {
			log.Printf("message %v dispatched", message.eventType)
		}
	}
}
