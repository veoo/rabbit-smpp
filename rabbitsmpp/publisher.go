package rabbitsmpp

import (
	"encoding/json"
	"errors"
	"sync"

	"github.com/streadway/amqp"
)

const (
	defaultExchange = ""
)

var (
	errAlreadyBound = errors.New("already bound")
	errClientClosed = errors.New("closed client")
	errMaxBindretry = errors.New("max bind retry")
)

type PublishOptionSetter func(*publishOptions)

type publishOptions struct {
	exchange string
}

func SetExchange(e string) PublishOptionSetter {
	return func(o *publishOptions) {
		o.exchange = e
	}
}

// Publisher is interface for publishing data to the queues
type Publisher interface {
	Publish(Job) error
	Close() error
}

func runWithRecovery(runCmd func() error, onError func() error) error {
	if err := runCmd(); err == nil {
		return nil
	}

	if err := onError(); err != nil {
		return err
	}

	return runCmd()

}

type publisher struct {
	m             *sync.Mutex
	client        Client
	clientFactory ClientFactory
	sendChan      Channel
	queueName     string
	exchange      string
}

// NewPublisher creates a new publisher with direct exchange
func NewPublisher(conf Config, options ...PublishOptionSetter) (Publisher, error) {
	clientFactory := defaultClientFactory(conf)
	return newPublisherWithClientFactory(clientFactory, options...)
}

func newPublisherWithClientFactory(clientFactory ClientFactory, options ...PublishOptionSetter) (Publisher, error) {
	var m sync.Mutex
	client, err := clientFactory()
	if err != nil {
		return nil, err
	}
	o := buildPublishOptions(options...)

	publisher := &publisher{
		m:             &m,
		client:        client,
		clientFactory: clientFactory,
		queueName:     client.Config().QueueName,
		exchange:      o.exchange,
	}

	if err := publisher.queueSetup(); err != nil {
		return nil, err
	}

	return publisher, nil
}

func (p *publisher) Close() error {
	p.m.Lock()
	defer p.m.Unlock()
	if p.sendChan != nil {
		p.sendChan.Close()
	}
	return nil
}

func (p *publisher) queueSetup() error {
	ch, err := p.client.Channel()
	if err != nil {
		p.Close()
		return err
	}

	p.sendChan = ch

	_, err = ch.QueueDeclare(
		p.queueName, // name
		true,        // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		p.Close()
		return err
	}

	return nil
}

// Publish sends the Job to the rabbitmq if the underlying connection needs re-binding it is done automagically
func (p *publisher) Publish(j Job) error {
	bodyBytes, err := json.Marshal(j)
	if err != nil {
		return err
	}

	cmd := func() error {
		p.m.Lock()
		defer p.m.Unlock()

		return p.sendChan.Publish(
			p.exchange,  // exchange
			p.queueName, // routing key
			false,       // mandatory
			false,       // immediate
			amqp.Publishing{
				ContentType:  "application/json",
				Body:         bodyBytes,
				DeliveryMode: uint8(2), // persistent
			})
	}

	return runWithRecovery(cmd, func() error {
		return p.reset()
	})
}

func (p *publisher) reset() error {
	client, err := p.clientFactory()
	if err != nil {
		return err
	}
	p.client = client

	// setup the queues
	return p.queueSetup()
}

func buildPublishOptions(options ...PublishOptionSetter) *publishOptions {
	o := &publishOptions{
		exchange: defaultExchange,
	}
	for _, option := range options {
		option(o)
	}
	return o
}
