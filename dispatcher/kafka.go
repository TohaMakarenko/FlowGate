package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/TohaMakarenko/FlowGate/shared"
	"github.com/segmentio/kafka-go"
	"log"
	"sync"
	"time"
)

type KafkaMessageQueue struct {
	reader            *kafka.Reader
	retryWriter       *kafka.Writer
	lastBatch         []kafka.Message
	messagesToRetry   []kafka.Message
	muMessagesToRetry sync.Mutex
}

func NewKafkaMessageQueue() *KafkaMessageQueue {
	const topic, retryTopic, addr string = "main-events", "retry-events", "localhost:9092"
	// make a new reader that consumes from topic-A, partition 0, at offset 42
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{addr},
		GroupID:   "dispatcher",
		Topic:     topic,
		Partition: 0,
		MaxBytes:  100e6, // 100MB
	})

	writer := kafka.Writer{
		Addr:                   kafka.TCP(addr),
		Topic:                  retryTopic,
		AllowAutoTopicCreation: true,
	}

	queue := &KafkaMessageQueue{
		reader:      reader,
		retryWriter: &writer,
	}
	return queue
}

func (queue *KafkaMessageQueue) GetMessagesBatch(ctx context.Context, size int) (messages []*shared.Message, ok bool) {
	cancel := func() {}
	// in func to call cancel from context
	defer func() { cancel() }()
	for i := 0; i < size; i++ {
		// consider select with ctx.Done?
		kMsg, err := queue.reader.FetchMessage(ctx)

		// break cycle if timeout
		if err != nil && errors.Is(err, ctx.Err()) {
			break
		} else if err != nil {
			log.Println("failed reading message:", err)
			return messages, false
		}

		queue.lastBatch = append(queue.lastBatch, kMsg)

		var msg *shared.Message
		msg, ok = deserializeMsg(kMsg)
		if ok {
			messages = append(messages, msg)

			// after first message use timeout, to not block already read messages
			if i == 0 {
				ctx, cancel = context.WithTimeout(ctx, 1*time.Second)
			}
		} else {
			queue.AddMessagesValueToRetry([][]byte{kMsg.Value})
		}

	}

	return messages, true
}

func (queue *KafkaMessageQueue) CommitMessagesBatch(ctx context.Context) bool {
	if len(queue.lastBatch) == 0 {
		return true
	}

	if len(queue.messagesToRetry) > 0 {
		err := queue.retryWriter.WriteMessages(ctx, queue.messagesToRetry...)
		if err != nil {
			log.Printf("failed writing messages to retry, \n%v", err)
			return false
		}
		log.Printf("Messages pushed to retry %v", len(queue.messagesToRetry))
		queue.messagesToRetry = make([]kafka.Message, 0)
	}

	err := queue.reader.CommitMessages(ctx, queue.lastBatch...)
	if err != nil {
		log.Printf("failed commiting last batch")
		return false
	}
	queue.lastBatch = make([]kafka.Message, 0)

	return true
}

func (queue *KafkaMessageQueue) AddMessagesToRetry(messages []*shared.Message) bool {
	if len(messages) == 0 {
		return true
	}
	values := make([][]byte, len(messages))
	for i, message := range messages {
		if value, ok := serializeMessage(message); ok {
			values[i] = value
		} else {
			return false
		}
	}
	return queue.AddMessagesValueToRetry(values)
}

func (queue *KafkaMessageQueue) AddMessagesValueToRetry(values [][]byte) bool {
	queue.muMessagesToRetry.Lock()
	defer queue.muMessagesToRetry.Unlock()
	kMsgs := make([]kafka.Message, len(values))
	for i, value := range values {
		kMsgs[i] = kafka.Message{Value: value}
	}
	queue.messagesToRetry = append(queue.messagesToRetry, kMsgs...)
	return true
}

func deserializeMsg(kMsg kafka.Message) (msg *shared.Message, ok bool) {
	msg = new(shared.Message)
	if err := json.Unmarshal(kMsg.Value, msg); err != nil {
		log.Printf("failed to unmarshal message at offset %d, key %v. Error: %v", kMsg.Offset, string(kMsg.Key), err)
		return nil, false
	}
	return msg, true
}

func serializeMessage(msg *shared.Message) (res []byte, ok bool) {
	result, err := json.Marshal(msg)
	if err != nil {
		log.Printf("failed serializing message")
		return nil, false
	}
	return result, true
}

func (queue *KafkaMessageQueue) Close() {
	if err := queue.reader.Close(); err != nil {
		log.Fatal("failed to close reader: ", err)
	}
}
