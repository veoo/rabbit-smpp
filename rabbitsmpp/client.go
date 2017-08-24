package rabbitsmpp

import (
	"errors"
	"log"
	"sync"

	"github.com/cenk/backoff"
	"github.com/streadway/amqp"
)

type conn struct {
	sync.Mutex
	conn *amqp.Connection
	url  string
	once *sync.Once
}

var (
	singleConn = &conn{once: &sync.Once{}}
)

func initConn(url string) error {
	var err error
	initFunc := func() {
		if singleConn.conn != nil {
			return
		}
		err = startConn(url)
	}
	singleConn.once.Do(initFunc)
	// if we fail to init, dont block this func
	if err != nil {
		singleConn.once = &sync.Once{}
	}
	return err
}

func startConn(url string) error {
	c, err := amqp.Dial(url)
	if err != nil {
		return err
	}
	singleConn.Lock()
	singleConn.conn = c
	singleConn.url = url
	singleConn.Unlock()
	rebindOnClose()
	return nil
}

func rebindOnClose() {
	singleConn.Lock()
	errors := make(chan *amqp.Error)
	errChan := singleConn.conn.NotifyClose(errors)
	url := singleConn.url
	singleConn.Unlock()
	go func() {
		closeNotification := <-errChan
		log.Println("connection was closed:", closeNotification, "rebinding...")
		CloseConn()

		ticker := backoff.NewTicker(backoff.NewExponentialBackOff())
		var err error
		for _ = range ticker.C {
			err = startConn(url)
			if err != nil {
				log.Println("failed to restore connection:", err)
				continue
			}
			log.Println("connection was restored")

			// stop the ticker
			ticker.Stop()
			return
		}
		log.Fatal("Failed to rebind connection:", err)
	}()
}

func CloseConn() error {
	singleConn.Lock()
	defer singleConn.Unlock()
	if singleConn.conn == nil {
		return nil
	}
	err := singleConn.conn.Close()
	singleConn.conn = nil
	return err
}

func getConn() *amqp.Connection {
	singleConn.Lock()
	defer singleConn.Unlock()
	return singleConn.conn
}

type Config struct {
	URL       string
	QueueName string
}

type Closer interface {
	// Close terminates the connection.
	Close() error
}

type Channel interface {
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	QueueDeclare(string, bool, bool, bool, bool, amqp.Table) (amqp.Queue, error)
	QueueInspect(string) (amqp.Queue, error)
	QueuePurge(string, bool) (int, error)
	Consume(string, string, bool, bool, bool, bool, amqp.Table) (<-chan amqp.Delivery, error)
	Publish(string, string, bool, bool, amqp.Publishing) error
	Qos(int, int, bool) error
	Closer
}

type Client interface {
	Channel() (Channel, error)
	Config() Config
	GetCloseChan() chan *amqp.Error
	Closer
}

type ClientFactory func() (Client, error)

func clientFactory(conf Config) ClientFactory {
	return func() (Client, error) {
		return NewClient(conf)
	}
}

var defaultClientFactory = clientFactory

type client struct {
	config Config
}

func NewClient(conf Config) (Client, error) {
	err := initConn(conf.URL)
	if err != nil {
		return nil, err
	}
	return &client{config: conf}, nil
}

func (c *client) Channel() (Channel, error) {
	singleConn.Lock()
	defer singleConn.Unlock()
	if singleConn.conn == nil {
		return nil, errors.New("connection is closed")
	}
	return singleConn.conn.Channel()
}

func (c *client) Config() Config {
	return c.config
}

func (c *client) Close() error {
	return getConn().Close()
}

func (c *client) GetCloseChan() chan *amqp.Error {
	singleConn.Lock()
	defer singleConn.Unlock()
	if singleConn.conn == nil {
		return nil
	}
	ch := make(chan *amqp.Error)
	return singleConn.conn.NotifyClose(ch)
}
