package shared

type Message struct {
	Id        string
	EventType string
	Body      []byte
	ApiToken  string
}
