package main

import (
	"context"
	"encoding/json"
	"github.com/TohaMakarenko/FlowGate/shared"
	"github.com/segmentio/kafka-go"
	"log"
	"net/http"
	"testing"
	"time"
)

func TestProcess(t *testing.T) {
	const msgCount int = 10
	waitChan := make(chan bool, 10)
	ctx, cancelCtx := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelCtx()

	handler := httpHandler{waitChan: waitChan}
	srv := &http.Server{Addr: ":5555"}
	http.Handle("/foo", &handler)

	go func() {
		err := srv.ListenAndServe()
		if err != nil {
			waitChan <- false
			t.Error("Unable to start http server: ", err)
		}
	}()

	writer := kafka.Writer{
		Addr:                   kafka.TCP("localhost:9092"),
		Topic:                  "main-events",
		Balancer:               &kafka.LeastBytes{},
		RequiredAcks:           kafka.RequireNone,
		AllowAutoTopicCreation: true,
	}

	messages := make([]kafka.Message, msgCount)
	for i := 0; i < msgCount; i++ {
		msg := shared.Message{
			EventType: "foo",
			Body:      []byte("{\"name\": \"bar\"}"),
			ApiToken:  "kekw",
		}
		val, _ := json.Marshal(msg)
		kMsg := kafka.Message{
			Value: val,
		}
		messages[i] = kMsg
	}

	err := writer.WriteMessages(ctx, messages...)
	log.Print("Messages pushed")
	if err != nil {
		t.Fatal("failed sending kafka message: ", err)
	}

	writer.Close()

	for i := 0; i < msgCount; i++ {
		select {
		case <-ctx.Done():
			t.Fatal("Timeout")
		case res := <-waitChan:
			if !res {
				t.Fatal("failed listening")
			}
		}
	}
}

type httpHandler struct {
	waitChan chan bool
}

func (h httpHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	log.Print("http request received")
	h.waitChan <- true
}
