package rabbitsmpp

import (
	"encoding/json"
	"sync"
	"testing"

	"golang.org/x/net/context"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type ConsumerSuite struct {
	suite.Suite
}

func TestConsumerSuite(t *testing.T) {
	s := &ConsumerSuite{}
	suite.Run(t, s)
}

func (s *ConsumerSuite) SetupTest() {}

func (s *ConsumerSuite) TearDownTest() {}

func (s *ConsumerSuite) Test_buildConsumeOptions() {
	options := []ConsumeOptionSetter{}

	o := buildConsumeOptions(options...)
	assert.Equal(s.T(), defaultPrefetchCount, o.prefetchCount)
	assert.Equal(s.T(), defaultPrefetchSize, o.prefetchSize)
	assert.Equal(s.T(), defaultGlobalQos, o.globalQos)

	o = buildConsumeOptions()
	assert.Equal(s.T(), defaultPrefetchCount, o.prefetchCount)
	assert.Equal(s.T(), defaultPrefetchSize, o.prefetchSize)
	assert.Equal(s.T(), defaultGlobalQos, o.globalQos)

	count := 10
	o = buildConsumeOptions(SetPrefetchCount(count))
	assert.Equal(s.T(), count, o.prefetchCount)
	assert.Equal(s.T(), defaultPrefetchSize, o.prefetchSize)
	assert.Equal(s.T(), defaultGlobalQos, o.globalQos)

	size := 20
	globalQos := false
	o = buildConsumeOptions(SetPrefetchCount(count), SetPrefetchSize(size), SetGlobalQos(globalQos))
	assert.Equal(s.T(), count, o.prefetchCount)
	assert.Equal(s.T(), size, o.prefetchSize)
	assert.Equal(s.T(), globalQos, o.globalQos)
}

func (s *ConsumerSuite) createMockChannel(queueName string, deliveryHandler func() <-chan amqp.Delivery) *MockChannel {
	mockChannel := &MockChannel{}
	mockChannel.On("Qos", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockChannel.On("QueueDeclare", queueName, true, false, false, false, mock.Anything).Return(amqp.Queue{
		Name: queueName,
	}, nil)

	mockChannel.On("Consume", queueName, "", false, false, false, false, mock.Anything).Return(
		func(string, string, bool, bool, bool, bool, amqp.Table) <-chan amqp.Delivery {
			return deliveryHandler()
		}, nil)

	return mockChannel

}

func (s *ConsumerSuite) Test_StartStop() {

	queueName := "consumerQueue"
	mockClient := &MockClient{}

	ctx, cancel := context.WithCancel(context.Background())
	consumer, err := NewConsumerWithContext(queueName, ctx, func() (Client, error) {
		return mockClient, nil
	})

	mockClient.On("QueueName").Return(queueName)
	mockClient.On("Bind").Return(make(chan *amqp.Error), nil)

	deliveryHandler := func() <-chan amqp.Delivery {
		deliveryChan := make(chan amqp.Delivery)

		go func() {
			m := map[string]string{"systemID": "vodafone"}
			job := Job{
				Attributes: NewAttributes(m),
			}

			bodyBytes, err := json.Marshal(job)
			assert.NoError(s.T(), err)

			delivery := amqp.Delivery{
				Body: bodyBytes,
			}

			deliveryChan <- delivery

		}()

		return deliveryChan

	}

	mockChannel := s.createMockChannel(queueName, deliveryHandler)
	mockClient.On("Channel").Return(mockChannel, nil)

	assert.NoError(s.T(), err)
	jobChan, errChan, err := consumer.Consume()
	assert.NoError(s.T(), err)

	// Consume whatever you expect here
	job := <-jobChan
	assert.Equal(s.T(), job.Attributes.Get("systemID"), "vodafone")

	var wg sync.WaitGroup
	wg.Add(2)
	mockChannel.On("Close").Return(nil)

	go func() {
		for _ = range jobChan {
			s.T().Fatal("We don't expect more jobs here sorry !")
		}
		wg.Done()

		for _ = range errChan {
			s.T().Fatal("We dont expect any error sorry !")
		}
		wg.Done()

	}()

	// now stop the consumer and the WaitGroup should finish as well because jobChan will be closed
	cancel()
	wg.Wait()
	// Do a few assertions here to make sure !

	mockClient.AssertNumberOfCalls(s.T(), "Bind", 1)
	mockClient.AssertNumberOfCalls(s.T(), "Channel", 1)

	mockChannel.AssertNumberOfCalls(s.T(), "Close", 1)
	mockChannel.AssertNumberOfCalls(s.T(), "Consume", 1)
	mockChannel.AssertNumberOfCalls(s.T(), "Qos", 1)
	mockChannel.AssertNumberOfCalls(s.T(), "QueueDeclare", 1)

}

func (s *ConsumerSuite) Test_BindRetrySucc() {

	queueName := "consumerQueue"
	mockClient := &MockClient{}

	ctx, cancel := context.WithCancel(context.Background())
	consumer, err := NewConsumerWithContext(queueName, ctx, func() (Client, error) {
		return mockClient, nil
	})

	mockClient.On("QueueName").Return(queueName)
	closeChan := make(chan *amqp.Error)
	mockClient.On("GetCloseChan").Return(closeChan)
	mockClient.On("Config").Return(Config{})

	deliveryHandler := func() <-chan amqp.Delivery {
		deliveryChan := make(chan amqp.Delivery)
		return deliveryChan
	}

	mockChannel := s.createMockChannel(queueName, deliveryHandler)
	mockClient.On("Channel").Return(mockChannel, nil)

	assert.NoError(s.T(), err)
	jobChan, errChan, err := consumer.Consume()
	assert.NoError(s.T(), err)

	// Try to send a close error so the consumer can re-bind
	var bindWg sync.WaitGroup
	bindWg.Add(1)
	go func() {
		closeChan <- &amqp.Error{}
		bindWg.Done()
	}()

	// wait for this to finish
	bindWg.Wait()

	// Stop the whole thing
	var wg sync.WaitGroup
	wg.Add(2)
	mockChannel.On("Close").Return(nil)

	go func() {
		for _ = range jobChan {
			s.T().Fatal("We don't expect more jobs here sorry !")
		}
		wg.Done()

		for _ = range errChan {
			s.T().Fatal("We dont expect any error sorry !")
		}
		wg.Done()

	}()

	// now stop the consumer and the WaitGroup should finish as well because jobChan will be closed
	cancel()
	wg.Wait()
	// Do a few assertions here to make sure !

	mockClient.AssertNumberOfCalls(s.T(), "Bind", 2)
	mockClient.AssertNumberOfCalls(s.T(), "Channel", 2)

	mockChannel.AssertNumberOfCalls(s.T(), "Close", 1)
	mockChannel.AssertNumberOfCalls(s.T(), "Consume", 2)
	mockChannel.AssertNumberOfCalls(s.T(), "Qos", 2)
	mockChannel.AssertNumberOfCalls(s.T(), "QueueDeclare", 2)

}
