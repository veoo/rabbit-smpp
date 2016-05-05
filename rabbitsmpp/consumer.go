package rabbitsmpp

import (
	"log"

	"github.com/veoo/go-smpp/smpp/pdu"
)

type Consumer interface {
	Consume() (chan pdu.Body, error)
	Client
}

type consumer struct {
	*client
}

func NewConsumer(conf Config) (Consumer, error) {
	c := NewClient(conf).(*client)
	return &consumer{c}, nil
}

func (c *consumer) Consume() (chan pdu.Body, error) {
	ch, err := c.Channel()
	if err != nil {
		return nil, err
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		c.QueueName(), // name
		false,         // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	if err != nil {
		return nil, err
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ackey
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		return nil, err
	}

	pduChan := make(chan pdu.Body)
	go func() {
		for d := range msgs {
			p, err := pdu.UnmarshalPDU(d.Body)
			if err != nil {
				log.Printf("unable to unmarshal PDU from json:", err)
			} else {
				log.Printf("fetched: %s", d.Body)
				pduChan <- p
				d.Ack(false)
			}
		}
	}()

	return pduChan, err
}
