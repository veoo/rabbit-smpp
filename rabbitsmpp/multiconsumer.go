package rabbitsmpp

import (
	"errors"
	"sync"

	"golang.org/x/net/context"
)

var (
	errConsumerDuplicate = errors.New("consumer already exist")
	errConsumerMissing   = errors.New("consumer missing")
)

type ConsumerContainer interface {
	Consumer
	AddFromConfig(Config) (string, error)
	AddRun(Consumer) error
	RemoveStop(string) error
	Exists(string) bool
}

// consumerContainer is a container that collects the data from all the consumer channels into 1
// channel it's a fan-in pattern. You can add an remove consumers dynamically
type consumerContainer struct {
	consumerClientFactory func(conf Config) ClientFactory
	consumers             map[string]Consumer
	m                     *sync.Mutex
	wg                    *sync.WaitGroup
	Ctx                   context.Context
	stopper               context.CancelFunc
	outJobChan            chan Job
	outErrChan            chan error
}

func NewContainer() ConsumerContainer {
	return NewContainerWithClientFactory(defaultClientFactory)
}

func NewContainerWithClientFactory(clientFactory func(conf Config) ClientFactory) ConsumerContainer {
	consumers := make(map[string]Consumer)
	var m sync.Mutex
	var wg sync.WaitGroup

	outJobChan := make(chan Job)
	outErrChan := make(chan error)

	ctx, cancel := context.WithCancel(context.Background())

	return &consumerContainer{
		consumerClientFactory: clientFactory,
		consumers:             consumers,
		m:                     &m,
		wg:                    &wg,
		Ctx:                   ctx,
		stopper:               cancel,
		outJobChan:            outJobChan,
		outErrChan:            outErrChan,
	}
}

func (container *consumerContainer) getNoLock(consumerID string) (Consumer, error) {
	consumer, ok := container.consumers[consumerID]
	if !ok {
		return nil, errConsumerMissing
	}

	return consumer, nil
}

func (container *consumerContainer) removeNoLock(consumerID string) error {

	if _, ok := container.consumers[consumerID]; !ok {
		return errConsumerMissing
	}

	delete(container.consumers, consumerID)
	return nil

}

func (container *consumerContainer) add(consumer Consumer) error {
	c, _ := container.getNoLock(consumer.ID())
	if c != nil {
		return errConsumerDuplicate
	}

	container.consumers[consumer.ID()] = consumer
	return nil
}

func (container *consumerContainer) Exists(consumerID string) bool {
	container.m.Lock()
	defer container.m.Unlock()

	_, ok := container.consumers[consumerID]
	return ok
}

func (container *consumerContainer) RemoveStop(consumerID string) error {
	container.m.Lock()
	defer container.m.Unlock()

	consumer, err := container.getNoLock(consumerID)
	if err != nil {
		return err
	}

	err = consumer.Stop()
	if err != nil {
		return err
	}

	err = container.removeNoLock(consumerID)
	if err != nil {
		return nil
	}

	return nil
}

func (container *consumerContainer) AddFromConfig(conf Config) (string, error) {
	consumer, err := NewConsumerWithContext(conf.QueueName, container.Ctx, container.consumerClientFactory(conf))
	if err != nil {
		return "", err
	}
	err = container.AddRun(consumer)
	if err != nil {
		return "", err
	}
	return consumer.ID(), err
}

func (container *consumerContainer) AddRun(consumer Consumer) error {
	container.m.Lock()
	defer container.m.Unlock()

	err := container.add(consumer)
	if err != nil {
		return err
	}

	container.wg.Add(1)
	go container.runConsumer(consumer)

	return nil
}

func (container *consumerContainer) runConsumer(consumer Consumer) {
	defer container.wg.Done()

	jChan, eChan, err := consumer.Consume()
	if err != nil {
		return
	}
	// If there is error we might remove the consumer ?
	for {
		select {
		case job, ok := <-jChan:
			if !ok {
				return
			}
			container.outJobChan <- job

		case err, ok := <-eChan:
			if !ok {
				// We disable that bit of the select statement
				eChan = nil
			} else {
				container.outErrChan <- err
			}

		}
	}
}

func (container *consumerContainer) Consume() (<-chan Job, <-chan error, error) {
	return container.outJobChan, container.outErrChan, nil
}

func (container *consumerContainer) ID() string {
	// We might want to return all of the queues so far ?
	return "Container"
}

// A Blocking operation waiting for all of the consumers to stop
func (container *consumerContainer) Stop() error {
	container.m.Lock()
	defer container.m.Unlock()

	container.stopper()
	container.wg.Wait()

	container.consumers = nil

	// Close the channels
	close(container.outJobChan)
	close(container.outErrChan)

	return nil
}
