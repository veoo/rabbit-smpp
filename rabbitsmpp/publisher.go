package rabbitsmpp

import (
	"encoding/json"
	"log"

	"github.com/streadway/amqp"
	"github.com/veoo/go-smpp/smpp/pdu"
)

type Publisher interface {
	Publish(pdu.Body) error
	Client
}

type publisher struct {
	*client
}

func NewPublisher(conf Config) (Publisher, error) {
	c := NewClient(conf).(*client)
	return &publisher{c}, nil
}

func (p *publisher) Publish(b pdu.Body) error {
	ch, err := p.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		p.QueueName(), // name
		false,         // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	if err != nil {
		return err
	}

	bodyBytes, err := json.Marshal(b)
	if err != nil {
		return err
	}
	err = ch.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        bodyBytes,
		})
	if err != nil {
		return err
	}

	log.Printf("Published %s", string(bodyBytes))

	return nil
}
