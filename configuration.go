package estuary

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
)

var Config Configuration

type Configuration struct {
	RabbitMQ RabbitMQ
	Workers int
}

type RabbitMQ struct {
	Host string
	Port string
	Vhost string
	Username string
	Password string
	Exchange string
}

func Configure(path string) (*Configuration, error) {
	log.Printf("Reading configuration from %s for %s environment", path, os.Getenv("ADISPATCH_ENV"))
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var c map[string]Configuration

	err = json.Unmarshal(b, &c)
	stage := os.Getenv("ADISPATCH_ENV")
	if stage == "" {
		stage = "development"
	}

	Config = c[stage]
	return &Config, err
}