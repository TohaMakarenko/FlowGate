package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/TohaMakarenko/FlowGate/shared"
	"github.com/segmentio/kafka-go"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
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

	kMsgs := make([]kafka.Message, msgCount)
	messages := make([]shared.Message, msgCount)
	for i := 0; i < msgCount; i++ {
		msg := shared.Message{
			Id:        strconv.Itoa(rand.Int()),
			EventType: "foo",
			Body:      []byte("{\"name\": \"bar\"}"),
			ApiToken:  "kekw",
			CreatedAt: time.Now(),
		}
		messages[i] = msg
		val, _ := json.Marshal(msg)
		kMsg := kafka.Message{
			Value: val,
		}
		kMsgs[i] = kMsg
	}

	err := writer.WriteMessages(ctx, kMsgs...)
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

	conn, ok := connectTestToClickHouse()
	if !ok {
		t.Fatal("failed connecting to clickhouse")
	}

	databaseCheck := checkMessagesInDb(ctx, conn, messages)
	if !databaseCheck {
		t.Fatal("database check failed")
	}
}

type httpHandler struct {
	waitChan chan bool
}

func (h httpHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	log.Print("http request received")
	h.waitChan <- true
}

func checkMessagesInDb(ctx context.Context, conn driver.Conn, sentMessages []shared.Message) bool {
	messageIds := Map(sentMessages, func(msg shared.Message) string { return msg.Id })
	messageIdsParameter := clickhouse.Named("ids", "['"+strings.Join(messageIds, "','")+"']")

	var readMessages []shared.Message
	err := conn.Select(ctx, &readMessages, `SELECT Id, EventType, emptyArrayUInt8() as Body, ApiToken, CreatedAt 
FROM messages 
where Id in {ids:Array(String)}`,
		messageIdsParameter)

	if err != nil {
		log.Printf("failed reading messages from clickhouse, %v", err)
		return false
	}

	for _, msg := range sentMessages {
		savedMsg := Find(readMessages, func(rMsg shared.Message) bool { return rMsg.Id == msg.Id })
		if savedMsg == nil {
			log.Printf("message for id %v not found in database", msg.Id)
			return false
		}
	}

	var readMessageResults []MessageResult
	err = conn.Select(ctx, &readMessageResults, `SELECT MessageId, ResultCode, emptyArrayUInt8() as ResponseBody, Error, CreatedAt 
FROM message_results 
WHERE MessageId in {ids:Array(String)}`,
		messageIdsParameter)

	if err != nil {
		log.Printf("failed reading message results from clickhouse, %v", err)
		return false
	}

	for _, msg := range sentMessages {
		savedMsg := Find(readMessageResults, func(msgRes MessageResult) bool { return msgRes.MessageId == msg.Id })
		if savedMsg == nil {
			log.Printf("message result for message id %v not found in database", msg.Id)
			return false
		}
	}

	log.Printf("DB check passed")
	return true
}

func connectTestToClickHouse() (conn driver.Conn, ok bool) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "changeme",
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout:     time.Second * 10,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
	})

	if err != nil {
		log.Fatalf("Failed to connect to ClickHouse: %v", err)
		return nil, false
	}

	// Ping to verify conn
	ctx := context.Background()

	if err := conn.Ping(ctx); err != nil {
		log.Fatalf("Failed to connect to ClickHouse: %v", err)
		return nil, false
	}
	fmt.Println("Connected to ClickHouse successfully!")

	return conn, true
}
