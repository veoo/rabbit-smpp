package rabbitsmpp

import (
	"encoding/json"
	"errors"
	"sync"

	"github.com/streadway/amqp"
)

var (
	errAlreadyBound = errors.New("already bound")
	errClientClosed = errors.New("closed client")
	errMaxBindretry = errors.New("max bind retry")
)

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
}

// NewPublisher creates a new publisher with direct exchange
func NewPublisher(conf Config) (Publisher, error) {
	clientFactory := defaultClientFactory(conf)
	return newPublisherWithClientFactory(clientFactory)
}

func newPublisherWithClientFactory(clientFactory ClientFactory) (Publisher, error) {
	var m sync.Mutex
	client, err := clientFactory()
	if err != nil {
		return nil, err
	}
	publisher := &publisher{
		m:             &m,
		client:        client,
		clientFactory: clientFactory,
		queueName:     client.Config().QueueName,
	}

	if err := publisher.queueSetup(); err != nil {
		return nil, err
	}

	return publisher, nil
}

func (p *publisher) Close() error {
	p.m.Lock()
	defer p.m.Unlock()
	p.sendChan.Close()
	p.client.Close()
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
	p.m.Lock()
	defer p.m.Unlock()

	bodyBytes, err := json.Marshal(j)
	if err != nil {
		return err
	}

	cmd := func() error {
		return p.sendChan.Publish(
			"",          // exchange
			p.queueName, // routing key
			false,       // mandatory
			false,       // immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        bodyBytes,
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

type DelayedPublisher interface {
	Publisher
	PublishWithDelay(Job, int) error
}

type delayedPublisher struct {
	m             *sync.Mutex
	clientFactory ClientFactory
	client        Client
	sendChan      Channel
	queueName     string

	delayExchange string
	delayMS       int
}

// NewDelayedPublisher works the same way as the normal publisher except it pushes
// its messages are sent to a delayExchange which keeps them there for delayMS time and then redirects
// them to the conf.QueueName so consumers can start consuming them
func NewDelayedPublisher(conf Config, delayExchange string, delayMS int) (DelayedPublisher, error) {
	clientFactory := defaultClientFactory(conf)
	return newDelayedPublisherWithClientFactory(clientFactory, delayExchange, delayMS)
}

func newDelayedPublisherWithClientFactory(clientFactory ClientFactory, delayExchange string, delayMS int) (DelayedPublisher, error) {
	var m sync.Mutex
	client, err := clientFactory()
	if err != nil {
		return nil, err
	}
	publisher := &delayedPublisher{
		m:             &m,
		client:        client,
		clientFactory: clientFactory,
		queueName:     client.Config().QueueName,
		delayExchange: delayExchange,
		delayMS:       delayMS,
	}

	if err := publisher.queueSetup(); err != nil {
		return nil, err
	}

	return publisher, nil
}

func (p *delayedPublisher) reset() error {
	client, err := p.clientFactory()
	if err != nil {
		return err
	}
	p.client = client

	// setup the queues
	return p.queueSetup()
}

func (p *delayedPublisher) queueSetup() error {
	ch, err := p.client.Channel()
	if err != nil {
		p.Close()
		return err
	}

	p.sendChan = ch

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
		p.Close()
		return err
	}

	q, err := ch.QueueDeclare(
		p.queueName, // name
		true,        // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		return err
	}

	err = ch.QueueBind(
		q.Name,          // queue name
		p.queueName,     // routing key
		p.delayExchange, // exchange
		false,
		nil)

	if err != nil {
		p.Close()
		return err
	}

	return nil
}

func (p *delayedPublisher) Close() error {
	p.m.Lock()
	defer p.m.Unlock()
	p.sendChan.Close()
	p.client.Close()
	return nil
}

func (p *delayedPublisher) Publish(j Job) error {
	return p.publish(j, p.delayMS)
}

func (p *delayedPublisher) PublishWithDelay(j Job, delayMs int) error {
	return p.publish(j, delayMs)
}

func (p *delayedPublisher) publish(j Job, delayMs int) error {
	p.m.Lock()
	defer p.m.Unlock()

	bodyBytes, err := json.Marshal(j)
	if err != nil {
		return err
	}

	cmd := func() error {
		return p.sendChan.Publish(
			p.delayExchange, // exchange
			p.queueName,     // routing key
			false,           // mandatory
			false,           // immediate
			amqp.Publishing{
				Headers: amqp.Table{
					"x-delay": int32(delayMs),
				},
				ContentType: "application/json",
				Body:        bodyBytes,
			})
	}

	return runWithRecovery(cmd, func() error {
		return p.reset()
	})
}
