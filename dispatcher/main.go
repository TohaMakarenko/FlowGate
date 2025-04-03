package main

import (
	"context"
)

func main() {
	queue := NewKafkaMessageQueue()
	defer queue.Close()
	var configRepo IDispatchingConfigRepository = NewFileDispatchingConfigRepository()
	msgRepo, ok := NewClickHouseRepository()
	if !ok {
		return
	}
	var dispatcher = NewHttpMessageDispatcher(queue, configRepo, msgRepo)
	defer dispatcher.Close()

	if ok {
		ctx := context.Background()
		dispatcher.Start(ctx)
	}
}
