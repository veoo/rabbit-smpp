package rabbitsmpp

import (
	"encoding/json"
	"errors"
	"log"
)

type Consumer interface {
	Consume() (chan Job, error)
	Stop() error
	Client
}

type consumer struct {
	*client
	stop    chan bool
	channel Channel
}

func NewConsumer(conf Config) (Consumer, error) {
	c := NewClient(conf).(*client)
	stop := make(chan bool)
	return &consumer{c, stop, nil}, nil
}

func (c *consumer) Consume() (chan Job, error) {
	if c.channel != nil {
		return nil, errors.New("consumer already active")
	}
	ch, err := c.Channel()
	if err != nil {
		return nil, err
	}
	c.channel = ch

	q, err := c.channel.QueueDeclare(
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

	msgs, err := c.channel.Consume(
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

func (c *consumer) sendStop() {
	c.stop <- true
}

func (c *consumer) Stop() error {
	if c.channel == nil {
		return nil
	}
	c.sendStop()
	err := c.channel.Close()
	c.channel = nil
	return err
}

func (c *consumer) Close() error {
	if c.channel == nil {
		return nil
	}
	go c.sendStop()
	c.channel = nil
	return c.conn.Close()
}
