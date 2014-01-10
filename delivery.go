package estuary

import (
	"github.com/streadway/amqp"
	"time"
)

type Delivery struct {
	RoutingKey string
	Body[]byte
	Timestamp time.Time
	amqpDelivery amqp.Delivery
}

func NewDelivery(d amqp.Delivery) *Delivery{
	return &Delivery{
		RoutingKey: d.RoutingKey,
		Body: d.Body,
		Timestamp: d.Timestamp,
		amqpDelivery: d,
	}
}

func (d *Delivery) Ack(b bool) {
	d.amqpDelivery.Ack(false)
}
