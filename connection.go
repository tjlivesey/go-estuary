package estuary

import (
	"github.com/streadway/amqp"
	"log"
	"fmt"
)

type Connection struct {
	amqpConnection *amqp.Connection
}

func NewConnection() (*Connection, error){
	config := Config.RabbitMQ
	log.Printf("%s", config)
	uri := fmt.Sprintf("amqp://%s:%s@%s:%s/%s", config.Username, config.Password, config.Host, config.Port, config.Vhost)
	log.Printf("Connecting to RabbitMQ at %s", uri)
	
	connection := new(Connection)
	amqp, err := amqp.Dial(uri)
	if err != nil {
		return nil, err
	}
	connection.amqpConnection = amqp
	return connection, err
}

func(c *Connection) Close() {
	log.Printf("Closing connection")
	c.amqpConnection.Close()
}