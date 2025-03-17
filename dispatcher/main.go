package dispatcher

import "context"

func main() {
	var queue IMessageQueue = NewKafkaMessageQueue()
	var configRepo IDispatchingConfigRepository = NewFileDispatchingConfigRepository()
	var dispatcher IMessageDispatcher = NewHttpMessageDispatcher(configRepo)

	ctx := context.Background()
	go queue.Start(ctx)
	go dispatcher.Start(ctx, queue.GetMessagesChannel())
}
