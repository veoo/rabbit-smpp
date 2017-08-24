package rabbitsmpp

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/streadway/amqp"
	"golang.org/x/net/context"
)

const (
	defaultPrefetchCount = 10
	defaultPrefetchSize  = 0
	defaultGlobalQos     = false
)

type ConsumeOptionSetter func(*consumeOptions)

type consumeOptions struct {
	prefetchCount int
	prefetchSize  int
	globalQos     bool
}

func SetPrefetchCount(n int) ConsumeOptionSetter {
	return func(o *consumeOptions) {
		o.prefetchCount = n
	}
}

func SetPrefetchSize(n int) ConsumeOptionSetter {
	return func(o *consumeOptions) {
		o.prefetchSize = n
	}
}

func SetGlobalQos(a bool) ConsumeOptionSetter {
	return func(o *consumeOptions) {
		o.globalQos = a
	}
}

type Consumer interface {
	Consume() (<-chan Job, <-chan error, error)
	Stop() error
	ID() string
}

type consumer struct {
	Client
	clientFactory ClientFactory
	channel       Channel
	ctx           context.Context
	cancel        context.CancelFunc
	prefetchCount int
	prefetchSize  int
	globalQos     bool
	queueName     string
	m             *sync.RWMutex
}

func buildConsumeOptions(options ...ConsumeOptionSetter) *consumeOptions {
	o := &consumeOptions{
		prefetchCount: defaultPrefetchCount,
		prefetchSize:  defaultPrefetchSize,
		globalQos:     defaultGlobalQos,
	}
	for _, option := range options {
		option(o)
	}
	return o
}

func NewConsumer(conf Config, options ...ConsumeOptionSetter) (Consumer, error) {
	clientFactory := defaultClientFactory(conf)
	ctx, _ := context.WithCancel(context.Background())

	return NewConsumerWithContext(conf.QueueName, ctx, clientFactory, options...)
}

func NewConsumerWithContext(queueName string, ctx context.Context, clientFactory ClientFactory, options ...ConsumeOptionSetter) (Consumer, error) {
	client, err := clientFactory()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	o := buildConsumeOptions(options...)

	return &consumer{
		Client:        client,
		clientFactory: clientFactory,
		ctx:           ctx,
		cancel:        cancel,
		prefetchCount: o.prefetchCount,
		prefetchSize:  o.prefetchSize,
		globalQos:     o.globalQos,
		queueName:     queueName,
		m:             &sync.RWMutex{},
	}, nil
}

func (c *consumer) ID() string {
	return c.queueName
}

func (c *consumer) waitOnClosedClient() {
	client, err := c.clientFactory()
	for err != nil {
		log.Println("Failed to recreate client:", err)
		time.Sleep(5 * time.Second)
		client, err = c.clientFactory()
	}
	c.Client = client
}

func (c *consumer) getConsumeChannel() (<-chan amqp.Delivery, error) {
	c.m.Lock()
	defer c.m.Unlock()

	ch, err := c.Channel()
	if err != nil {
		return nil, err
	}
	err = ch.Qos(c.prefetchCount, c.prefetchSize, c.globalQos)
	if err != nil {
		return nil, err
	}

	c.channel = ch

	q, err := c.channel.QueueDeclare(
		c.queueName, // name
		true,        // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
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
	if c.getChannel() != nil {
		return nil, nil, errors.New("consumer already active")
	}
	closeChan := c.Client.GetCloseChan()
	dlvChan, err := c.getConsumeChannel()
	if err != nil {
		return nil, nil, err
	}
	jobChan := make(chan Job)
	errChan := make(chan error)

	go func() {
		defer func() {
			c.m.Lock()
			defer c.m.Unlock()
			if c.channel != nil {
				_ = c.channel.Close()
				c.channel = nil
			}

			close(jobChan)
			close(errChan)
		}()

		for {
			err = c.consume(dlvChan, closeChan, jobChan)
			// if consume returns without an error, means that it was terminated
			// properly, otherwise something went wrong and it needs to restart
			if err == nil {
				log.Printf("EOF consuming for: %s", c.ID())
				return
			}
			log.Println("stopped consuming jobs:", err)

			// we need this because sometimes we don't have a listener here so we don't
			// want to block the whole consuming because we weren't able to send an error
			select {
			case errChan <- err:
			default:
				log.Println("no listener errChan skipping")
			}

			c.waitOnClosedClient()
			closeChan = c.Client.GetCloseChan()
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
		case d, ok := <-dlvChan:
			if !ok {
				log.Printf("deliver chan is closed, returning")
				return nil
			}
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
	if c.getChannel() == nil {
		return nil
	}
	// Sends the stop signal
	c.cancel()
	return nil
}

func (c *consumer) getChannel() Channel {
	c.m.RLock()
	defer c.m.RUnlock()
	return c.channel
}
