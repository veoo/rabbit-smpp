package rabbitsmpp

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
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

func (c *consumer) bindWithRetry() chan *amqp.Error {
	closeChan, err := c.Bind()
	for err != nil {
		log.Println("Failed to bind consumer:", err)
		time.Sleep(5 * time.Second)
		closeChan, err = c.Bind()
	}
	return closeChan
}

func (c *consumer) getConsumeChannel() (<-chan amqp.Delivery, error) {
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

	return c.channel.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ackey
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
}

func (c *consumer) Consume() (<-chan Job, <-chan error, error) {
	if c.channel != nil {
		return nil, nil, errors.New("consumer already active")
	}
	closeChan, err := c.Bind()
	if err != nil {
		return nil, nil, err
	}
	dlvChan, err := c.getConsumeChannel()
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
			err = c.consume(dlvChan, closeChan, jobChan)
			// if consume returns without an error, means that it was terminated
			// properly, otherwise something went wrong and it needs to restart
			if err == nil {
				log.Printf("EOF consuming for: %s", c.ID())
				return
			}
			errChan <- err
			log.Println("stopped consuming jobs:", err)
			closeChan = c.bindWithRetry()
			dlvChan, err = c.getConsumeChannel()
			for err != nil {
				time.Sleep(5 * time.Second)
				dlvChan, err = c.getConsumeChannel()
			}
		}
	}()

	return jobChan, errChan, nil
}

func (c *consumer) consume(dlvChan <-chan amqp.Delivery, closeChan <-chan *amqp.Error, jobChan chan<- Job) error {
	for {
		select {
		case d := <-dlvChan:
			j := Job{}
			err := json.Unmarshal(d.Body, &j)
			if err != nil {
				return fmt.Errorf("failed to unmarshal PDU: %v", err)
			}
			j.delivery = &d
			jobChan <- j
		case err := <-closeChan:
			return err
		case <-c.ctx.Done():
			return nil
		}
	}
}

func (c *consumer) Stop() error {
	if c.channel == nil {
		return nil
	}
	// Sends the stop signal
	c.cancel()
	return nil
}
