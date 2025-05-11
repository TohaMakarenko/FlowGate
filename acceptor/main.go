package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"log/slog"
)

func main() {
	config, err := readConfig()

	if err != nil {
		panic(err)
	}

	InitSubmitter(config)

	router := gin.New()

	slog.Info("Starting acceptor application")

	InitRouter(router)

	serveUrl := config.Host + ":" + config.Port
	slog.Info("Starting HTTP server on " + serveUrl)

	err = router.Run(serveUrl)

	if err != nil {
		slog.Error("Failed to start server.", "error", err)
		panic(err)
	}
}

func readConfig() (*acceptorConfig, error) {
	var config acceptorConfig

	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("./resources/")
	viper.AddConfigPath("/app/resources/")

	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Failed reading config file: %w", err))
		return nil, err
	}

	if err := viper.UnmarshalKey("acceptor", &config); err != nil {
		slog.Error("Failed to read acceptor config.", "error", err)
		return nil, err
	}

	return &config, nil
}
