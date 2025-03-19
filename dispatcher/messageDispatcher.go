package main

import (
	"context"
	"github.com/TohaMakarenko/FlowGate/dispatcher/shared"
)

type IMessageDispatcher interface {
	Start(ctx context.Context, msgChanel chan *shared.Message)
}
