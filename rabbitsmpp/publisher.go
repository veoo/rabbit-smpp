package rabbitsmpp

import (
	"encoding/json"
	"errors"
	"strconv"
	"sync"

	"github.com/cenk/backoff"
	"github.com/streadway/amqp"
)

var (
	errAlreadyBound = errors.New("already bound")
	errClientClosed = errors.New("closed client")
	errMaxBindretry = errors.New("max bind retry")
)

// PublisherClient is same as Client interface but also support Rebind operation
type PublisherClient interface {
	Client
	ReBind() (PublisherClient, chan *amqp.Error, error)
}

type publisherClient struct {
	sm *sync.Mutex

	config   Config
	ampqConn *amqp.Connection
	bound    bool

	closed bool
}

func NewPublisherClient(conf Config) (*publisherClient, error) {
	var m sync.Mutex

	return &publisherClient{
		sm:     &m,
		config: conf,
	}, nil
}

func (c *publisherClient) Bind() (chan *amqp.Error, error) {
	c.sm.Lock()
	defer c.sm.Unlock()

	if c.bound {
		return nil, errAlreadyBound
	}

	if c.closed {
		return nil, errClientClosed
	}

	conn, err := amqp.Dial(c.config.URL)
	if err != nil {
		return nil, err
	}

	errors := make(chan *amqp.Error)
	errChan := conn.NotifyClose(errors)
	c.ampqConn = conn

	c.bound = true
	return errChan, nil
}

func (c *publisherClient) Channel() (Channel, error) {
	c.sm.Lock()
	defer c.sm.Unlock()

	if c.closed {
		return nil, errClientClosed
	}

	return c.ampqConn.Channel()
}

func (c *publisherClient) ReBind() (PublisherClient, chan *amqp.Error, error) {
	c.sm.Lock()
	defer c.sm.Unlock()

	ticker := backoff.NewTicker(backoff.NewExponentialBackOff())
	for _ = range ticker.C {
		newClient, err := NewPublisherClient(c.config)
		if err != nil {
			continue
		}

		errChan, err := newClient.Bind()
		if err != nil {
			continue
		}

		// stop the ticker
		ticker.Stop()
		return newClient, errChan, nil
	}

	return nil, nil, errClientClosed

}

func (c *publisherClient) Close() error {
	c.sm.Lock()
	defer c.sm.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true
	c.ampqConn.Close()
	return nil
}

func (c *publisherClient) QueueName() string {
	c.sm.Lock()
	defer c.sm.Unlock()

	return c.config.QueueName
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
	m        *sync.Mutex
	client   PublisherClient
	sendChan Channel
}

// NewPublisher creates a new publisher with direct exchange
func NewPublisher(conf Config) (Publisher, error) {
	pubClient, err := NewPublisherClient(conf)
	if err != nil {
		return nil, err
	}

	return newPublisherWithClient(pubClient)
}

func newPublisherWithClient(client PublisherClient) (Publisher, error) {
	var m sync.Mutex

	publisher := &publisher{
		m:      &m,
		client: client,
	}

	// setup all the connections here
	if err := publisher.reset(false); err != nil {
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
		p.client.QueueName(), // name
		true,                 // durable
		false,                // delete when unused
		false,                // exclusive
		false,                // no-wait
		nil,                  // arguments
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
			"",                   // exchange
			p.client.QueueName(), // routing key
			false,                // mandatory
			false,                // immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        bodyBytes,
			})
	}

	return runWithRecovery(cmd, func() error {
		return p.reset(true)
	})
}

func (p *publisher) reset(rebind bool) error {

	if !rebind {
		_, err := p.client.Bind()
		if err != nil {
			return err
		}
	} else {
		client, _, err := p.client.ReBind()
		if err != nil {
			return err
		}

		p.client = client
	}

	// setup the queues
	return p.queueSetup()
}

type DelayedPublisher interface {
	Publisher
	PublishWithDelay(Job, int) error
}

type delayedPublisher struct {
	m        *sync.Mutex
	client   PublisherClient
	sendChan Channel

	delayExchange string
	delayMS       int
}

// NewDelayedPublisher works the same way as the normal publisher except it pushes
// its messages are sent to a delayExchange which keeps them there for delayMS time and then redirects
// them to the conf.QueueName so consumers can start consuming them
func NewDelayedPublisher(conf Config, delayExchange string, delayMS int) (DelayedPublisher, error) {
	client, err := NewPublisherClient(conf)
	if err != nil {
		return nil, err
	}

	return newDelayedPublisherWithClient(client, delayExchange, delayMS)
}

func newDelayedPublisherWithClient(client PublisherClient, delayExchange string, delayMS int) (DelayedPublisher, error) {
	var m sync.Mutex

	publisher := &delayedPublisher{
		m:             &m,
		client:        client,
		delayExchange: delayExchange,
		delayMS:       delayMS,
	}

	// setup all the connections here
	if err := publisher.reset(false); err != nil {
		return nil, err
	}

	return publisher, nil
}

func (p *delayedPublisher) reset(rebind bool) error {

	if !rebind {
		_, err := p.client.Bind()
		if err != nil {
			return err
		}
	} else {
		client, _, err := p.client.ReBind()
		if err != nil {
			return err
		}

		p.client = client
	}

	// setup the queues
	if err := p.queueSetup(); err != nil {
		return err
	}

	return nil
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
		return err
	}

	q, err := ch.QueueDeclare(
		p.client.QueueName(), // name
		true,                 // durable
		false,                // delete when unused
		false,                // exclusive
		false,                // no-wait
		nil,                  // arguments
	)
	if err != nil {
		return err
	}

	err = ch.QueueBind(
		q.Name,               // queue name
		p.client.QueueName(), // routing key
		p.delayExchange,      // exchange
		false,
		nil)

	if err != nil {
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
			p.delayExchange,      // exchange
			p.client.QueueName(), // routing key
			false,                // mandatory
			false,                // immediate
			amqp.Publishing{
				Headers: amqp.Table{
					"x-delay": strconv.Itoa(delayMs),
				},
				ContentType: "application/json",
				Body:        bodyBytes,
			})
	}

	return runWithRecovery(cmd, func() error {
		return p.reset(true)
	})
}
