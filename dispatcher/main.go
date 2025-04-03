package main

import (
	"context"
)

func main() {
	var queue IMessageQueue = NewKafkaMessageQueue()
	defer queue.Close()
	var configRepo IDispatchingConfigRepository = NewFileDispatchingConfigRepository()
	var msgRepo IMessageRepository
	var ok bool
	msgRepo, ok = NewClickHouseRepository()
	if !ok {
		return
	}
	var dispatcher IMessageDispatcher = NewHttpMessageDispatcher(queue, configRepo, msgRepo)
	defer dispatcher.Close()

	if ok {
		ctx := context.Background()
		dispatcher.Start(ctx)
	}
}
