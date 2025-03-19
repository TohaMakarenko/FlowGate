package shared

type Message struct {
	EventType string
	Body      []byte
	ApiToken  string
}
