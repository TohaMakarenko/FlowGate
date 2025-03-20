package main

import (
	"context"
	"encoding/json"
	"github.com/TohaMakarenko/FlowGate/shared"
	"github.com/segmentio/kafka-go"
	"log"
)

type KafkaMessageQueue struct {
	connection      *kafka.Conn
	messagesChannel chan *shared.Message
}

func NewKafkaMessageQueue() *KafkaMessageQueue {
	queue := &KafkaMessageQueue{messagesChannel: make(chan *shared.Message)}
	return queue
}

func (queue *KafkaMessageQueue) GetMessagesChannel() chan *shared.Message {
	return queue.messagesChannel
}

func (queue *KafkaMessageQueue) Start(ctx context.Context) {
	const topic string = "topic-A"
	// make a new reader that consumes from topic-A, partition 0, at offset 42
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		GroupID:   "dispatcher",
		Topic:     topic,
		Partition: 0,
		MaxBytes:  100e6, // 100MB
	})
	defer func() {
		if err := reader.Close(); err != nil {
			log.Fatal("failed to close reader: ", err)
		}
	}()
	log.Printf("Start listening kafka topic %v", topic)

	//reader.SetOffset(0) // todo offset?

	defer close(queue.messagesChannel)
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Fatal("failed reading message from kafka: ", err)
		}
		var message *shared.Message = &shared.Message{}
		if err = json.Unmarshal(msg.Value, message); err != nil {
			log.Printf("failed to unmarshal message at offset %d, key %v. Error: %v", msg.Offset, string(msg.Key), err)
			continue
		}
		queue.messagesChannel <- message
		if err = reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("Error committing message: %v", err)
		} else {
			log.Printf("message commited at offset %d: %s = %s\n", msg.Offset, string(msg.Key), string(msg.Value))
		}
	}
}
