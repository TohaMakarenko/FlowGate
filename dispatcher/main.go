package main

import (
	"context"
)

func main() {
	var queue IMessageQueue = NewKafkaMessageQueue()
	var configRepo IDispatchingConfigRepository = NewFileDispatchingConfigRepository()
	var msgRepo IMessageRepository = MessageRepository{}
	var dispatcher IMessageDispatcher = NewHttpMessageDispatcher(queue, configRepo, msgRepo)

	ctx := context.Background()
	dispatcher.Start(ctx)
}
