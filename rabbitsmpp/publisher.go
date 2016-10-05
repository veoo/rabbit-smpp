package rabbitsmpp

import (
	"encoding/json"
	"strconv"

	"github.com/streadway/amqp"
)

type Publisher interface {
	Publish(Job) error
}

type publisher struct {
	*client
}

func NewPublisher(conf Config) (Publisher, error) {
	c := NewClient(conf).(*client)
	return &publisher{c}, nil
}

func (p *publisher) Publish(j Job) error {
	_, err := p.Bind()
	if err != nil {
		return err
	}
	defer p.conn.Close()

	ch, err := p.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		p.QueueName(), // name
		true,          // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	if err != nil {
		return err
	}

	bodyBytes, err := json.Marshal(j)
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

	return nil
}

type delayedPublisher struct {
	*client
	delayExchange string
	delayMS       int
}

// NewDelayedPublisher works the same way as the normal publisher except it pushes
// its messages are sent to a delayExchange which keeps them there for delayMS time and then redirects
// them to the conf.QueueName so consumers can start consuming them
func NewDelayedPublisher(conf Config, delayExchange string, delayMS int) (Publisher, error) {
	c := NewClient(conf).(*client)
	return &delayedPublisher{
		c,
		delayExchange,
		delayMS,
	}, nil
}

func (p *delayedPublisher) Publish(j Job) error {
	_, err := p.Bind()
	if err != nil {
		return err
	}
	defer p.conn.Close()

	ch, err := p.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	// Create a delayed exchange
	err = ch.ExchangeDeclare(
		p.delayExchange,
		"x-delayed-message",
		true,
		false,
		false,
		true,
		amqp.Table{
			"x-delayed-type": "direct",
		},
	)

	if err != nil {
		return err
	}

	q, err := ch.QueueDeclare(
		p.QueueName(), // name
		true,          // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	if err != nil {
		return err
	}

	err = ch.QueueBind(
		q.Name,          // queue name
		p.QueueName(),   // routing key
		p.delayExchange, // exchange
		false,
		nil)

	if err != nil {
		return err
	}

	bodyBytes, err := json.Marshal(j)
	if err != nil {
		return err
	}

	return ch.Publish(
		p.delayExchange, // exchange
		p.QueueName(),   // routing key
		false,           // mandatory
		false,           // immediate
		amqp.Publishing{
			Headers: amqp.Table{
				"x-delay": strconv.Itoa(p.delayMS),
			},
			ContentType: "application/json",
			Body:        bodyBytes,
		})
}
