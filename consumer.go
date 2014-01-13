package estuary

import (
	"github.com/streadway/amqp"
	"fmt"
)

type Consumer struct {
	AmqpChannel *amqp.Channel
	Handler Handler
}

func NewConsumer(connection *amqp.Connection, handler Handler) (*Consumer, error) {
	consumer := new(Consumer)
	consumer.Handler = handler

	// Create channel
	var err error
	consumer.AmqpChannel, err = connection.Channel()
	if err != nil {
		return nil, fmt.Errorf("Error creating channel for RabbitMQ connection %s", err)
	}

	// Declare exchange
	exchangeName := Config.RabbitMQ.Exchange
	err = consumer.AmqpChannel.ExchangeDeclare(
    exchangeName, // name of the exchange
    "topic", 			// type
    true,         // durable
    false,        // delete when complete
    false,        // internal
    false,        // noWait
    nil,          // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Error declaring exchange %s: %s", exchangeName, err)
	}

	// Create queue and bind
	queue, err := consumer.AmqpChannel.QueueDeclare(
	    handler.Name, // name of the queue
	    true,      // durable
	    false,     // delete when usused
	    handler.Exclusive, // exclusive
	    false,     // noWait
	    nil,       // arguments
		)
	if err != nil {
		return nil, fmt.Errorf("Error creating queue %s: %s", handler.Name, err)
	}

	err = consumer.AmqpChannel.QueueBind(
    queue.Name,
    handler.BindingKey, 
    exchangeName,   // sourceExchange
    false,      // noWait
    nil,        // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Could not bind queue: %s", err)
	}
	
  return consumer, err
}

func (c *Consumer) Start() (<-chan amqp.Delivery, error){
	deliveries, err := c.AmqpChannel.Consume(
    c.Handler.Name, // name
    "", // consumer tag
    false,      // noAck
    false,      // exclusive
    false,      // noLocal
    false,      // noWait
    nil,        // arguments
  )
  if err != nil {
  	return nil, err
  }
  return deliveries, err
}

func (c *Consumer) Stop() {
	c.AmqpChannel.Close()
}