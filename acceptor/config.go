package main

type acceptorConfig struct {
	Host        string `mapstructure:"host"`
	Port        string `mapstructure:"port"`
	KafkaUrl    string `mapstructure:"kafka-url"`
	EventsTopic string `mapstructure:"events-topic"`
}
