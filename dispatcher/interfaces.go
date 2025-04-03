package main

import (
	"context"
	"github.com/TohaMakarenko/FlowGate/shared"
)

type IMessageDispatcher interface {
	Start(ctx context.Context)
	Close()
}

type IMessageQueue interface {
	GetMessagesBatch(ctx context.Context, size int) (messages []*shared.Message, ok bool)
	CommitMessagesBatch(ctx context.Context) bool
	AddMessagesToRetry(messages []*shared.Message) bool
	Close()
}

type IDispatchingConfigRepository interface {
	Get(eventType string) (cfg DispatchingConfig, ok bool)
}

type IMessageRepository interface {
	SaveMessages(ctx context.Context, messages []*shared.Message) bool
	SaveMessageResult(ctx context.Context, msgResult *MessageResult) bool
	Close()
}
