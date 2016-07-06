package rabbitsmpp

import (
	"encoding/json"
	"errors"
	"log"
)

type Consumer interface {
	Consume() (chan Job, chan error, error)
	Stop() error
	Client
}

type consumer struct {
	*client
	errChan chan error
	channel Channel
}

func NewConsumer(conf Config) (Consumer, error) {
	c := NewClient(conf).(*client)
	errChan := make(chan error)
	return &consumer{c, errChan, nil}, nil
}

func (c *consumer) Consume() (chan Job, chan error, error) {
	if c.channel != nil {
		return nil, nil, errors.New("consumer already active")
	}
	ch, err := c.Channel()
	if err != nil {
		return nil, nil, err
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
		return nil, nil, err
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
		return nil, nil, err
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
					c.stop("empty body in delivery, stopping consumption")
					return
				}
				j.delivery = &d
				jobChan <- j
			case <-c.errChan:
				return
			}
		}
	}()

	return jobChan, c.errChan, nil
}

func (c *consumer) sendStop(msg string) {
	c.errChan <- errors.New(msg)
}

func (c *consumer) stop(msg string) error {
	if c.channel == nil {
		return nil
	}
	c.sendStop(msg)
	err := c.channel.Close()
	c.channel = nil
	return err
}

func (c *consumer) Stop() error {
	return c.stop("stop received")
}

func (c *consumer) Close() error {
	if c.channel == nil {
		return nil
	}
	go c.sendStop("close received")
	c.channel = nil
	return c.conn.Close()
}
