package main

import (
	"context"
)

func main() {
	var queue IMessageQueue = NewKafkaMessageQueue()
	var configRepo IDispatchingConfigRepository = NewFileDispatchingConfigRepository()
	var dispatcher IMessageDispatcher = NewHttpMessageDispatcher(configRepo)

	ctx := context.Background()
	go queue.Start(ctx)
	dispatcher.Start(ctx, queue.GetMessagesChannel())
}
