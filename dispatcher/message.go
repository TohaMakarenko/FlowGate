package dispatcher

type Message struct {
	eventType string
	body      string
	apiToken  []byte
}
