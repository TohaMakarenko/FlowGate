package main

import (
	"gopkg.in/yaml.v3"
	"log"
	"os"
)

type FileDispatchingConfigRepository struct {
	config map[string]DispatchingConfig
}

type yamlDispatchingConfig struct {
	TargetEndpoint string            `yaml:"targetEndpoint"`
	StaticParams   map[string]string `yaml:"staticParams"`
}

func NewFileDispatchingConfigRepository() *FileDispatchingConfigRepository {
	configs := readYamlConfig()
	repo := FileDispatchingConfigRepository{config: make(map[string]DispatchingConfig)}
	for key, config := range configs {
		repo.config[key] = DispatchingConfig{
			eventType:      key,
			targetEndpoint: config.TargetEndpoint,
			staticParams:   config.StaticParams,
		}
	}

	return &repo
}

func readYamlConfig() map[string]yamlDispatchingConfig {
	const fileName string = "sampleConfig.yaml"
	yamlFile, err := os.ReadFile(fileName)
	if err != nil {
		log.Fatalf("failed reading dispatching config file: %v", err)
	}
	var configs map[string]yamlDispatchingConfig
	err = yaml.Unmarshal(yamlFile, &configs)
	if err != nil {
		log.Fatalf("failed unmarshalling yaml: %v", err)
	}
	return configs
}

func (repo *FileDispatchingConfigRepository) Get(eventType string) (cfg DispatchingConfig, ok bool) {
	cfg, ok = repo.config[eventType]
	return
}
