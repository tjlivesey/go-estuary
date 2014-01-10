package estuary

import (
	"encoding/json"
	"io/ioutil"
	"log"
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

func Configure(path, environment string) (*Configuration, error) {
	log.Printf("Reading configuration from %s", path)
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(b, &Config)
	return &Config, err
}