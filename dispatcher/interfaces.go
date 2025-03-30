package main

import (
	"context"
	"github.com/TohaMakarenko/FlowGate/shared"
)

type IMessageDispatcher interface {
	Start(ctx context.Context)
}

type IMessageQueue interface {
	GetMessagesBatch(ctx context.Context, size int) (messages []shared.Message, ok bool)
	CommitMessagesBatch(ctx context.Context) bool
	AddMessageToRetry(msg *shared.Message) bool
}

type IDispatchingConfigRepository interface {
	Get(eventType string) (cfg DispatchingConfig, ok bool)
}

type IMessageRepository interface {
	SaveMessage(msg *shared.Message) bool
	SaveMessageResult(msgResult *MessageResult) bool
}
