package rabbitsmpp

import (
	"fmt"

	"github.com/streadway/amqp"
)

type Config struct {
	User      string
	Password  string
	Host      string
	QueueName string
}

type Closer interface {
	// Close terminates the connection.
	Close() error
}

type Client interface {
	Bind() (chan *amqp.Error, error)
	Channel() (*amqp.Channel, error)
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
	address := fmt.Sprintf("amqp://%s:%s@%s", c.config.User, c.config.Password, c.config.Host)
	conn, err := amqp.Dial(address)
	if err != nil {
		return nil, err
	}
	c.conn = conn
	errors := make(chan *amqp.Error)
	return conn.NotifyClose(errors), nil
}

func (c *client) Channel() (*amqp.Channel, error) {
	return c.conn.Channel()
}

func (c *client) Close() error {
	return c.conn.Close()
}

func (c *client) QueueName() string {
	return c.config.QueueName
}
