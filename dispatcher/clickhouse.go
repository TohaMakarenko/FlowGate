package main

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/TohaMakarenko/FlowGate/shared"
	"github.com/spf13/viper"
	"log"
	"time"
)

type ClickHouseRepository struct {
	conn driver.Conn
}

func NewClickHouseRepository(ctx context.Context) (repo *ClickHouseRepository, ok bool) {
	conn, ok := connectToClickHouse(ctx)
	if !ok {
		return nil, false
	}

	if err := createTables(conn, ctx); err != nil {
		log.Fatalf("Failed to create tables: %v", err)
		return nil, false
	}

	repo = &ClickHouseRepository{}
	repo.conn = conn
	return repo, true
}

func (repo *ClickHouseRepository) Close() {
	err := repo.conn.Close()
	if err != nil {
		log.Printf("Failed closing ClickHouse connection")
	}
}

func (repo *ClickHouseRepository) SaveMessages(ctx context.Context, messages []*shared.Message) bool {
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

func (repo *ClickHouseRepository) SaveMessageResult(ctx context.Context, msgResult *MessageResult) bool {
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

func getClickhouseOptions() (options *clickhouse.Options, ok bool) {
	var config clickhouseConfig
	if err := viper.UnmarshalKey("messagesStorage.config", &config); err != nil {
		log.Printf("Failed reading clickhouse config, %v", err)
		return nil, false
	}

	// Parse durations
	dialTimeout, _ := time.ParseDuration(config.DialTimeout)
	connMaxLifetime, _ := time.ParseDuration(config.ConnMaxLifetime)

	return &clickhouse.Options{
		Addr: config.Addr,
		Auth: clickhouse.Auth{
			Database: config.Auth.Database,
			Username: config.Auth.Username,
			Password: config.Auth.Password,
		},
		Settings:        config.Settings,
		DialTimeout:     dialTimeout,
		MaxOpenConns:    config.MaxOpenConns,
		MaxIdleConns:    config.MaxIdleConns,
		ConnMaxLifetime: connMaxLifetime,
	}, true
}

func connectToClickHouse(ctx context.Context) (conn driver.Conn, ok bool) {
	options, ok := getClickhouseOptions()
	if !ok {
		return nil, false
	}

	conn, err := clickhouse.Open(options)

	if err != nil {
		log.Fatalf("Failed to connect to ClickHouse: %v", err)
		return nil, false
	}

	if err := conn.Ping(ctx); err != nil {
		log.Fatalf("Failed to ping ClickHouse: %v", err)
		return nil, false
	}
	fmt.Println("Connected to ClickHouse successfully!")

	return conn, true
}

func createTables(conn driver.Conn, ctx context.Context) error {
	query := `
	CREATE TABLE IF NOT EXISTS messages (
		Id String,
		EventType String,
		Body String,
		ApiToken String,
		CreatedAt DateTime DEFAULT now()
	) ENGINE = MergeTree()
	ORDER BY (Id, EventType)`
	err := conn.Exec(ctx, query)
	if err != nil {
		return err
	}

	query = `
	CREATE TABLE IF NOT EXISTS message_results (
		MessageId String,
		ResultCode UInt16,
		ResponseBody String,
		Error String,
		CreatedAt DateTime DEFAULT now()
	) ENGINE = MergeTree()
	ORDER BY (MessageId, ResultCode) `
	err = conn.Exec(ctx, query)
	return err
}

type clickhouseConfig struct {
	Addr            []string       `mapstructure:"addr"`
	Auth            clickhouseAuth `mapstructure:"auth"`
	Settings        map[string]any `mapstructure:"settings"`
	DialTimeout     string         `mapstructure:"dial_timeout"`
	MaxOpenConns    int            `mapstructure:"max_open_conns"`
	MaxIdleConns    int            `mapstructure:"max_idle_conns"`
	ConnMaxLifetime string         `mapstructure:"conn_max_lifetime"`
}

type clickhouseAuth struct {
	Database string `mapstructure:"database"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}
