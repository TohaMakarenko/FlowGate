package dispatcher

import (
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
			log.Printf("failed to get config for %v", message.eventType)
		}
		_, err := http.Post(config.targetEndpoint, "application/json", nil)
		if err != nil {
			log.Printf("failed dispatching message %v", message.eventType)
		} else {
			log.Printf("message %v dispatched", message.eventType)
		}
	}
}
