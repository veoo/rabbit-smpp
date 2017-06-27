package rabbitsmpp

import (
	"log"
	"sync"

	"github.com/cenk/backoff"
	"github.com/streadway/amqp"
)

var conn struct {
	conn *amqp.Connection
	url  string
	sync.Mutex
}

func initConn(url string) error {
	conn.Lock()
	if conn.conn != nil {
		conn.Unlock()
		return nil
	}
	conn.Unlock()
	return startConn(url)
}

func startConn(url string) error {
	c, err := amqp.Dial(url)
	if err != nil {
		return err
	}
	conn.Lock()
	conn.conn = c
	conn.url = url
	conn.Unlock()
	rebindOnClose()
	return nil
}

func rebindOnClose() {
	conn.Lock()
	errors := make(chan *amqp.Error)
	errChan := conn.conn.NotifyClose(errors)
	conn.Unlock()
	go func() {
		closeNotification := <-errChan
		log.Println("connection was closed:", closeNotification, "rebinding...")

		ticker := backoff.NewTicker(backoff.NewExponentialBackOff())
		var err error
		for _ = range ticker.C {
			err = startConn(conn.url)
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

func closeConn() error {
	conn.Lock()
	defer conn.Unlock()
	if conn.conn == nil {
		return nil
	}
	err := conn.conn.Close()
	conn.conn = nil
	return err
}

func getConn() *amqp.Connection {
	conn.Lock()
	defer conn.Unlock()
	return conn.conn
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
	conn.Lock()
	defer conn.Unlock()
	return conn.conn.Channel()
}

func (c *client) Config() Config {
	return c.config
}

func (c *client) Close() error {
	return getConn().Close()
}

func (c *client) GetCloseChan() chan *amqp.Error {
	conn.Lock()
	defer conn.Unlock()
	if conn.conn == nil {
		return nil
	}
	ch := make(chan *amqp.Error)
	return conn.conn.NotifyClose(ch)
}
