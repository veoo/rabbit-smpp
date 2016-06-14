package rabbitsmpp

import (
	"github.com/streadway/amqp"
)

type Config struct {
	URL       string
	QueueName string
}

type Closer interface {
	// Close terminates the connection.
	Close() error
}

type Channel interface {
	QueueDeclare(string, bool, bool, bool, bool, amqp.Table) (amqp.Queue, error)
	QueueInspect(string) (amqp.Queue, error)
	Consume(string, string, bool, bool, bool, bool, amqp.Table) (<-chan amqp.Delivery, error)
	Publish(string, string, bool, bool, amqp.Publishing) error
	Closer
}

type Client interface {
	Bind() (chan *amqp.Error, error)
	Channel() (Channel, error)
	Closer
}

type client struct {
	config *Config
	conn   *amqp.Connection
}

func NewClient(conf Config) Client {
	return &client{config: &conf}
}

// Connect establishes the connection to the rabbit MQ
// it returns a channel that notifies the listener when the connection
// is closed either by an error/failure or correct shutdown
func (c *client) Bind() (chan *amqp.Error, error) {
	conn, err := amqp.Dial(c.config.URL)
	if err != nil {
		return nil, err
	}
	c.conn = conn
	errors := make(chan *amqp.Error)
	return conn.NotifyClose(errors), nil
}

func (c *client) Channel() (Channel, error) {
	return c.conn.Channel()
}

func (c *client) Close() error {
	return c.conn.Close()
}

func (c *client) QueueName() string {
	return c.config.QueueName
}
