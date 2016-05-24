package rabbitsmpp

import (
	"encoding/json"
	"log"
)

type Consumer interface {
	Consume() (chan Job, error)
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

func (c *consumer) Consume() (chan Job, error) {
	ch, err := c.Channel()
	if err != nil {
		return nil, err
	}

	q, err := ch.QueueDeclare(
		c.QueueName(), // name
		true,          // durable
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
	jobChan := make(chan Job)

	go func() {
		defer ch.Close()
		for {
			select {
			case d := <-msgs:
				j := Job{}
				err := json.Unmarshal(d.Body, &j)
				if err != nil {
					// TODO: Figure out what to do with this failed job
					log.Printf("failed to unmarshal PDU: %v", err)
					continue
				}
				j.delivery = &d
				jobChan <- j
			case <-c.stop:
				return
			}
		}
	}()

	return jobChan, nil
}

func (c *consumer) Close() error {
	c.stop <- true
	return c.client.Close()
}
