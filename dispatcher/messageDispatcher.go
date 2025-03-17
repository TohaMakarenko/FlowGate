package dispatcher

import "context"

type IMessageDispatcher interface {
	Start(ctx context.Context, msgChanel chan *Message)
}
