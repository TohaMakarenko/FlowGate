package main

import (
	"context"
	"github.com/TohaMakarenko/FlowGate/shared"
)

type IMessageQueue interface {
	Start(ctx context.Context)
	GetMessagesChannel() chan *shared.Message
}
