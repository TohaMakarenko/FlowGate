package main

import (
	"bytes"
	"context"
	"github.com/TohaMakarenko/FlowGate/shared"
	"io"
	"log"
	"math"
	"net/http"
	"sync"
	"time"
)

type HttpMessageDispatcher struct {
	queue      IMessageQueue
	msgRepo    IMessageRepository
	configRepo IDispatchingConfigRepository
}

func NewHttpMessageDispatcher(queue IMessageQueue, configRepo IDispatchingConfigRepository, msgRepo IMessageRepository) *HttpMessageDispatcher {
	dispatcher := HttpMessageDispatcher{
		queue:      queue,
		msgRepo:    msgRepo,
		configRepo: configRepo,
	}
	return &dispatcher
}

func (dispatcher *HttpMessageDispatcher) Start(ctx context.Context) {
	log.Print("Start listening")
	const concurrencyLimit = 10
	throttler := make(chan struct{}, int(math.Max(concurrencyLimit-1, 1)))
	for {
		messages, ok := dispatcher.getMessagesWithRetry(ctx)
		if !ok {
			break
		}
		ok = dispatcher.msgRepo.SaveMessages(ctx, messages)
		if !ok {
			dispatcher.queue.AddMessagesToRetry(messages)
			continue
		}
		wg := sync.WaitGroup{}
		wg.Add(len(messages))
		for _, msg := range messages {
			go func() {
				timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second) //todo timeout from config
				defer func() {
					cancel()
					wg.Done()
					throttler <- struct{}{}
				}()
				dispatcher.handleMessage(timeoutCtx, msg)
			}()
			<-throttler
		}
		wg.Wait()
		dispatcher.queue.CommitMessagesBatch(ctx)
	}
	log.Print("done dispatching messages")
}

func (dispatcher *HttpMessageDispatcher) Close() {
}

func (dispatcher *HttpMessageDispatcher) getMessagesWithRetry(ctx context.Context) (messages []*shared.Message, ok bool) {
	const maxBatch int = 100
	const maxAttempts int = 10
	retryAttempts := 0
	for {
		messages, ok := dispatcher.queue.GetMessagesBatch(ctx, maxBatch)
		if ok {
			log.Printf("Reveived %v messages", len(messages))
			return messages, true
		}
		retryAttempts++
		if retryAttempts >= maxAttempts {
			log.Printf("unable to get messages from queue after %v attempts", maxAttempts)
			break
		}
		time.Sleep(5 * time.Second)
		log.Printf("retry reat messages from queue, attempt %v", retryAttempts)
		continue
	}
	return nil, false
}

func (dispatcher *HttpMessageDispatcher) handleMessage(ctx context.Context, msg *shared.Message) {
	result, ok := dispatcher.sendMessageHttp(ctx, msg)
	if result != nil {
		dispatcher.msgRepo.SaveMessageResult(ctx, result)
	}
	if !ok {
		dispatcher.queue.AddMessagesToRetry([]*shared.Message{msg})
		return
	}

}

func (dispatcher *HttpMessageDispatcher) sendMessageHttp(ctx context.Context, msg *shared.Message) (result *MessageResult, ok bool) {
	config, ok := dispatcher.configRepo.Get(msg.EventType)
	if !ok {
		log.Printf("config for %v not found", msg.EventType)
		return nil, false
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, config.targetEndpoint, bytes.NewReader(msg.Body))
	if err != nil {
		log.Printf("failed creating request for msg %v. Error: %v", msg.EventType, err)
		return nil, false
	}

	resp, err := http.DefaultClient.Do(req)

	if err != nil {
		log.Printf("failed sending http msg %v. Error: %v", msg.EventType, err)
		return nil, false
	}

	result = &MessageResult{
		MessageId:  msg.Id,
		ResultCode: uint16(resp.StatusCode),
	}
	result.ResponseBody, _ = io.ReadAll(resp.Body)
	log.Printf("msg %v dispatched", msg.EventType)
	return result, true
}
