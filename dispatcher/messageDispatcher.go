package main

import (
	"context"
	"github.com/TohaMakarenko/FlowGate/shared"
)

type IMessageDispatcher interface {
	Start(ctx context.Context, msgChanel chan *shared.Message)
}
