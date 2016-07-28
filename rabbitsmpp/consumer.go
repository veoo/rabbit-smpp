package rabbitsmpp

import (
	"encoding/json"
	"errors"
	"log"

	"golang.org/x/net/context"
)

type Consumer interface {
	Consume() (<-chan Job, <-chan error, error)
	Stop() error
	ID() string
}

type consumer struct {
	*client
	channel Channel
	ctx     context.Context
	cancel  context.CancelFunc
}

func NewConsumer(conf Config) (Consumer, error) {
	c := NewClient(conf).(*client)
	ctx, cancel := context.WithCancel(context.Background())

	return &consumer{
		client:  c,
		channel: nil,
		ctx:     ctx,
		cancel:  cancel,
	}, nil
}

func NewConsumerWithContext(ctx context.Context, conf Config) (Consumer, error) {
	c := NewClient(conf).(*client)
	ctx, cancel := context.WithCancel(ctx)

	return &consumer{
		client:  c,
		channel: nil,
		ctx:     ctx,
		cancel:  cancel,
	}, nil
}

func (c *consumer) ID() string {
	return c.client.QueueName()
}

func (c *consumer) Consume() (<-chan Job, <-chan error, error) {
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
	errChan := make(chan error)

	go func() {
		defer func() {
			_ = c.channel.Close()
			c.channel = nil
		}()
		defer close(jobChan)
		defer close(errChan)

		for {
			select {
			case d := <-msgs:
				j := Job{}
				err := json.Unmarshal(d.Body, &j)
				if err != nil {
					// TODO: Figure out what to do with this failed job
					log.Printf("failed to unmarshal PDU: %v", err)
					errChan <- err
					return
				}
				j.delivery = &d
				jobChan <- j
			case <-c.ctx.Done():
				log.Println("EOF consuming for : %s", c.ID())
				return
			}
		}
	}()

	return jobChan, errChan, nil
}

func (c *consumer) Stop() error {
	if c.channel == nil {
		return nil
	}
	// Sends the stop signal
	c.cancel()
	return nil
}
