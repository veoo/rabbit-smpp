package rabbitsmpp

import (
	"encoding/json"
	"sync"

	"github.com/streadway/amqp"
)

type DelayedPublisher interface {
	Publisher
	PublishWithDelay(Job, int) error
}

type DelayedTTLPublisher interface {
	Publisher
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
				ContentType:  "application/json",
				Body:         bodyBytes,
				DeliveryMode: uint8(2), // persistent
			})
	}

	return runWithRecovery(cmd, func() error {
		return p.reset()
	})
}

type delayedTTLPublisher struct {
	m             *sync.Mutex
	clientFactory ClientFactory
	client        Client
	sendChan      Channel
	queueName     string

	delayQueue string
	delayMS    int
}

// NewDelayedPublisher works the same way as the normal publisher except it pushes
// its messages are sent to a delayQueue which keeps them there for delayMS time and then redirects
// them to the conf.QueueName so consumers can start consuming them
func NewDelayedTTLPublisher(conf Config, delayQueue string, delayMS int) (DelayedTTLPublisher, error) {
	clientFactory := defaultClientFactory(conf)
	return newDelayedTTLPublisherWithClientFactory(clientFactory, delayQueue, delayMS)
}

func newDelayedTTLPublisherWithClientFactory(clientFactory ClientFactory, delayQueue string, delayMS int) (DelayedTTLPublisher, error) {
	var m sync.Mutex
	client, err := clientFactory()
	if err != nil {
		return nil, err
	}
	publisher := &delayedTTLPublisher{
		m:             &m,
		client:        client,
		clientFactory: clientFactory,
		queueName:     client.Config().QueueName,
		delayQueue:    delayQueue,
		delayMS:       delayMS,
	}

	if err := publisher.queueSetup(); err != nil {
		return nil, err
	}

	return publisher, nil
}

func (p *delayedTTLPublisher) reset() error {
	client, err := p.clientFactory()
	if err != nil {
		return err
	}
	p.client = client

	// setup the queues
	return p.queueSetup()
}

func (p *delayedTTLPublisher) queueSetup() error {
	ch, err := p.client.Channel()
	if err != nil {
		p.Close()
		return err
	}

	p.sendChan = ch

	args := amqp.Table{
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": p.queueName,
		"x-message-ttl":             int32(p.delayMS),
	}

	_, err = ch.QueueDeclare(
		p.delayQueue, // name
		true,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		args,         // arguments
	)
	if err != nil {
		p.Close()
		return err
	}

	return nil
}

func (p *delayedTTLPublisher) Close() error {
	p.m.Lock()
	defer p.m.Unlock()
	p.sendChan.Close()
	return nil
}

func (p *delayedTTLPublisher) Publish(j Job) error {
	p.m.Lock()
	defer p.m.Unlock()

	bodyBytes, err := json.Marshal(j)
	if err != nil {
		return err
	}

	cmd := func() error {
		return p.sendChan.Publish(
			"",           // exchange
			p.delayQueue, // routing key
			false,        // mandatory
			false,        // immediate
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
