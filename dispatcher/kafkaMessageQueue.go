package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
)

type KafkaMessageQueue struct {
	connection      *kafka.Conn
	messagesChannel chan *Message
}

func NewKafkaMessageQueue() *KafkaMessageQueue {
	queue := &KafkaMessageQueue{messagesChannel: make(chan *Message)}
	return queue
}

func (queue *KafkaMessageQueue) GetMessagesChannel() chan *Message {
	return queue.messagesChannel
}

func (queue *KafkaMessageQueue) Start(ctx context.Context) {
	// make a new reader that consumes from topic-A, partition 0, at offset 42
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     "topic-A",
		Partition: 0,
		MaxBytes:  100e6, // 100MB
	})
	//reader.SetOffset(0) // todo offset?

	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			break
		}
		var message *Message = &Message{}
		if err = json.Unmarshal(m.Value, message); err != nil {
			log.Printf("failed to unmarshal message at offset %d, key %v", m.Offset, string(m.Key))
			continue
		}
		queue.messagesChannel <- message
		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}

	if err := reader.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}
