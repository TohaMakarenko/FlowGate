package main

import (
	"context"
	"github.com/TohaMakarenko/FlowGate/dispatcher/shared"
)

type IMessageQueue interface {
	Start(ctx context.Context)
	GetMessagesChannel() chan *shared.Message
}
