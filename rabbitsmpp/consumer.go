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
	stop chan bool
}

func NewConsumer(conf Config) (Consumer, error) {
	c := NewClient(conf).(*client)
	stop := make(chan bool)
	return &consumer{c, stop}, nil
}

func (c *consumer) Consume() (chan pdu.Body, error) {
	ch, err := c.Channel()
	if err != nil {
		return nil, err
	}

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
		defer ch.Close()
		for {
			select {
			case d := <-msgs:
				d.Ack(false)
				p, err := pdu.UnmarshalPDU(d.Body)
				if err != nil {
					// TODO: Figure out what to do with this failed job
					log.Printf("failed to unmarshal PDU: %v", err)
					continue
				}
				pduChan <- p
			case <-c.stop:
				return
			}
		}
	}()

	return pduChan, nil
}

func (c *consumer) Close() error {
	c.stop <- true
	return c.client.Close()
}
