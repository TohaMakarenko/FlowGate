package dispatcher

import "context"

type IMessageQueue interface {
	Start(ctx context.Context)
	GetMessagesChannel() chan *Message
}
