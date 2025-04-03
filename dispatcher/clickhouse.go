package main

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/TohaMakarenko/FlowGate/shared"
	"log"
	"time"
)

type ClickHouseRepository struct {
	conn driver.Conn
}

func NewClickHouseRepository() (repo *ClickHouseRepository, ok bool) {
	conn, ok := connectToClickHouse()
	if !ok {
		return nil, false
	}
	repo = &ClickHouseRepository{}
	repo.conn = conn
	return repo, true
}

func (repo ClickHouseRepository) Close() {
	err := repo.conn.Close()
	if err != nil {
		log.Printf("Failed closing ClickHouse connection")
	}
}

func (repo ClickHouseRepository) SaveMessages(ctx context.Context, messages []*shared.Message) bool {
	batch, err := repo.conn.PrepareBatch(ctx, "INSERT INTO messages (Id, EventType, Body, ApiToken, CreatedAt)")
	if err != nil {
		log.Printf("Failed prepearing messages batch for ClickHouse. %v", err)
		return false
	}

	for _, msg := range messages {
		if err := batch.Append(
			msg.Id,
			msg.EventType,
			string(msg.Body),
			msg.ApiToken,
			msg.CreatedAt,
		); err != nil {
			log.Printf("Failed appending message to batch batch for ClickHouse. %v", err)
			return false
		}
	}

	err = batch.Send()
	if err != nil {
		log.Printf("Failed sending messages batch to ClickHouse. %v", err)
		return false
	}
	return true
}

func (repo ClickHouseRepository) SaveMessageResult(ctx context.Context, msgResult *MessageResult) bool {
	batch, err := repo.conn.PrepareBatch(ctx, "INSERT INTO message_results (MessageId, ResultCode, ResponseBody, Error, CreatedAt)")
	if err != nil {
		log.Printf("Failed prepearing message result batch for ClickHouse. %v", err)
		return false
	}

	if err := batch.Append(
		msgResult.MessageId,
		msgResult.ResultCode,
		string(msgResult.ResponseBody),
		msgResult.Error,
		msgResult.CreatedAt,
	); err != nil {
		log.Printf("Failed appending message result to batch batch for ClickHouse. %v", err)
		return false
	}

	err = batch.Send()

	if err != nil {
		log.Printf("Failed sending message results batch to ClickHouse. %v", err)
		return false
	}
	return true
}

func connectToClickHouse() (conn driver.Conn, ok bool) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:18123"},
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

	if err = createTables(err, conn, ctx); err != nil {
		log.Fatalf("Failed to create tables: %v", err)
		return nil, false
	}
	return conn, true
}

func createTables(err error, conn driver.Conn, ctx context.Context) error {
	query := `
	CREATE TABLE IF NOT EXISTS messages (
		Id String,
		EventType String,
		Body String,
		ApiToken String,
		CreatedAt DateTime DEFAULT now()
	) ENGINE = MergeTree()
	ORDER BY (Id, EventType)`
	err = conn.Exec(ctx, query)
	if err != nil {
		return err
	}

	query = `
	CREATE TABLE IF NOT EXISTS message_results (
		MessageId String,
		ResultCode Int32,
		ResponseBody String,
		Error String,
		CreatedAt DateTime DEFAULT now()
	) ENGINE = MergeTree()
	ORDER BY (MessageId, ResultCode) `
	err = conn.Exec(ctx, query)
	return err
}
