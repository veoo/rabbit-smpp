package rabbitsmpp

import (
	"log"

	"github.com/veoo/go-smpp/smpp/pdu"
)

type Consumer interface {
	Consume(chan<- pdu.Body, chan<- error)
	Client
}

type consumer struct {
	*client
	stop chan bool
}

func NewConsumer(conf Config) (Consumer, error) {
	c := NewClient(conf).(*client)
	stop := make(chan bool)

	return &consumer{c, stop}, nil
}

func (c *consumer) Consume(pduChan chan<- pdu.Body, errChan chan<- error) {
	ch, err := c.Channel()
	if err != nil {
		errChan <- err
		return
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		c.QueueName(), // name
		false,         // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	if err != nil {
		errChan <- err
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ackey
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		errChan <- err
	}

	for {
		select {
		case d := <-msgs:
			d.Ack(false)
			p, err := pdu.UnmarshalPDU(d.Body)
			if err != nil {
				log.Printf("failed to unmarshal PDU: %v", err)
				errChan <- err
				continue
			}
			h := p.Header()
			log.Printf("fetched: Seq: %v, ID: %v", h.Seq, h.ID.String())
			pduChan <- p
		case <-c.stop:
			log.Printf("Stopping consumer")
			return
		}
	}

}

func (c *consumer) Close() error {
	c.stop <- true
	return c.client.Close()
}
