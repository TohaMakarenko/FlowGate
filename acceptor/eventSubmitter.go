package main

import (
	"context"
	"encoding/json"
	"github.com/TohaMakarenko/FlowGate/shared"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"log/slog"
	"time"
)

var producer *kafka.Writer

func InitSubmitter(config *acceptorConfig) {
	producer = &kafka.Writer{
		Addr:                   kafka.TCP(config.KafkaUrl),
		Topic:                  config.EventsTopic,
		AllowAutoTopicCreation: true, // todo: create topics via script based on env
	}
}

// how to gracefully handle kafka down?
func submitEventToQueue(ctx context.Context, userId int, eventType string, rawEvent []byte) (bool, error) {
	id := uuid.New().String()
	// todo: remove api token from message
	event := shared.Message{Id: id, EventType: eventType, Body: rawEvent, UserId: userId, CreatedAt: time.Now()}

	slog.Info("Submit event.", "id", id, "eventType", eventType, "userId", userId)

	err := submitToKafka(ctx, event)

	if err != nil {
		slog.Error("Failed to submit event to Queue.", "id", id, "eventType", eventType, "userId", userId, "error", err)
		return false, err
	}

	return true, nil
}

func submitToKafka(ctx context.Context, event shared.Message) error {
	serialized, _ := json.Marshal(event)

	msg := kafka.Message{Value: serialized}

	return producer.WriteMessages(ctx, msg)
}
