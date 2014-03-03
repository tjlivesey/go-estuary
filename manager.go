package estuary

import (
	"fmt"
	"log"
	"github.com/streadway/amqp"
	"sync"
	"time"
)

type Manager struct {
	Connection *Connection
	Consumers []Consumer
	wg sync.WaitGroup
}

func Start() (*Manager, error){
	if len(Handlers) == 0 {
		return nil, fmt.Errorf("No registered handlers")
	}
	
	manager := new(Manager)
	connection, err := NewConnection()
	if err != nil {
		return nil, fmt.Errorf("Error connecting to RabbitMQ: %s", err)
	}
	manager.Connection = connection

	// Iterate through registered handlers and create consumers
	for _, handler := range Handlers {
		consumer, err := NewConsumer(connection.amqpConnection, handler)
		if err != nil {
			return nil, err
		}
		manager.Consumers = append(manager.Consumers, *consumer)
	}
	log.Printf("Starting %d consumer(s)", len(manager.Consumers))

	for _,consumer := range manager.Consumers {
		deliveries, err := consumer.Start()
		if err != nil {
			return nil, err
		}
		go manager.handleDeliveries(deliveries, consumer)
	}
	return manager, nil
}

func (m *Manager) Stop() {
	m.Connection.Close()
	log.Printf("Waiting for handlers to finish current jobs")
	// Timeout channel so we only wait for 5secs max
	done := make(chan bool)
	timeout := time.After(5*time.Second)
	go func(){ 
		m.wg.Wait()
		done <- true
	}()
	select {
	case <- done:
		log.Printf("All jobs finished")
	case <- timeout:
		close(done)
		log.Printf("Jobs did not complete after 5 seconds, forcing quit")
	}
}

func (m *Manager) Shutdown() {
	m.Connection.Close()
	log.Printf("Waiting for handlers to finish current jobs")
	m.wg.Wait()
	log.Printf("All jobs finished")
}

func (m *Manager) handleDeliveries(deliveries <-chan amqp.Delivery, consumer Consumer){
	for d := range deliveries {
		delivery := NewDelivery(d)
		// Pass delivery to handler and recover if the handler panics
		go func(wg *sync.WaitGroup) {
			defer func(){
				if r := recover(); r != nil{
					log.Printf("Handler '%s' panicked while processing delivery: \n %s", consumer.Handler.Name, r)
				}
			}()
			wg.Add(1)
			consumer.Handler.HandlerFunc(*delivery)
			wg.Done()
		}(&m.wg)
		// Acknowledge message early rather than wait for response from 
		// handler. This could be tweaked later to allow retries but
		// handlers would have to be idempotent which is difficult for 
		// analaytics HTTP requests.
		delivery.Ack(false)
	}
}