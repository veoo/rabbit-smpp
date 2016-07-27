package rabbitsmpp


import (
	"golang.org/x/net/context"
	"sync"
	"github.com/go-errors/errors"
	_ "fmt"
)

var (
	errConsumerDuplicate = errors.New("consumer already exist")
	errConsumerMissing = errors.New("consumer missing")
)

// ConsumerContainer is a container that collects the data from all the consumer channels into 1
// channel it's a fan-in pattern. You can add an remove consumers dynamically
type ConsumerContainer struct {
	consumers  map[string]Consumer
	m          *sync.Mutex
	wg         *sync.WaitGroup
	Ctx        context.Context
	stopper    context.CancelFunc
	outJobChan chan Job
	outErrChan chan error
}

func NewContainer() *ConsumerContainer {
	consumers := make(map[string]Consumer)
	var m sync.Mutex
	var wg sync.WaitGroup

	outJobChan := make(chan Job)
	outErrChan := make(chan error)

	ctx, cancel := context.WithCancel(context.Background())

	return &ConsumerContainer{
		consumers:consumers,
		m: &m,
		wg: &wg,
		Ctx: ctx,
		stopper: cancel,
		outJobChan: outJobChan,
		outErrChan: outErrChan,
	}
}

func (container *ConsumerContainer) getNoLock(consumerID string) (Consumer, error) {
	consumer, ok := container.consumers[consumerID];
	if !ok {
		return nil, errConsumerMissing
	}

	return consumer, nil
}

func (container *ConsumerContainer) removeNoLock(consumerID string) error {

	if _, ok := container.consumers[consumerID]; !ok {
		return errConsumerMissing
	}

	delete(container.consumers, consumerID)
	return nil

}

func (container *ConsumerContainer) add(consumer Consumer) error {
	c, _ := container.getNoLock(consumer.ID())
	if c != nil {
		return errConsumerDuplicate
	}

	container.consumers[consumer.ID()] = consumer
	return nil
}

func (container *ConsumerContainer) RemoveStop(consumerID string) error {
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
	if err != nil{
		return nil
	}

	return nil
}

func (container *ConsumerContainer) AddRun(consumer Consumer) error {
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

func (container *ConsumerContainer) runConsumer(consumer Consumer) {
	defer container.wg.Done()

	jChan, eChan, err := consumer.Consume()
	if err != nil{
		return
	}
	// If there is error we might remove the consumer ?
	for {
		select {
		case job, ok := <- jChan:
			if !ok {
				//fmt.Println("Upstream Closed ", consumer.ID())
				return
			}
			container.outJobChan <- job

		case err, ok := <- eChan:
			if !ok{
				// We disable that bit of the select statement
				eChan = nil
			}else{
				container.outErrChan <- err
			}

		}
	}
}

func (container *ConsumerContainer) Consume() (<-chan Job, <-chan error, error) {
	return container.outJobChan, container.outErrChan, nil
}

func (container *ConsumerContainer) ID() string{
	// We might want to return all of the queues so far ?
	return "Container"
}

// A Blocking operation waiting for all of the consumers to stop
func (container *ConsumerContainer) Stop() error {
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
