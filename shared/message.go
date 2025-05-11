package shared

import "time"

type Message struct {
	Id        string
	EventType string
	Body      []byte
	UserId    int
	CreatedAt time.Time
}
