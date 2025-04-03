package shared

import "time"

type Message struct {
	Id        string
	EventType string
	Body      []byte
	ApiToken  string
	CreatedAt time.Time
}
