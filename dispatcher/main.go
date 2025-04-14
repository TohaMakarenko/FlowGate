package main

import (
	"context"
	"fmt"
	"github.com/spf13/viper"
	"log"
)

func main() {
	ctx := context.Background()

	ok := SetConfig()
	if !ok {
		return
	}

	queue := NewKafkaMessageQueue()
	defer queue.Close()
	var configRepo IDispatchingConfigRepository = NewFileDispatchingConfigRepository()
	msgRepo, ok := getMessagesRepository(ctx)
	if !ok {
		return
	}
	var dispatcher = NewHttpMessageDispatcher(queue, configRepo, msgRepo)
	defer dispatcher.Close()

	if ok {
		dispatcher.Start(ctx)
	}
}

func SetConfig() bool {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")

	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("failed reading config file: %w", err))
		return false
	}
	return true
}

func getMessagesRepository(ctx context.Context) (repo IMessageRepository, ok bool) {
	repoType := viper.GetString("messagesStorage.type")
	if repoType == "clickHouse" {
		return NewClickHouseRepository(ctx)
	} else {
		log.Printf("Unrecognized messagesStorage.type '%v'", repoType)
		return nil, false
	}
}
