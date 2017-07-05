package rabbitsmpp

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const (
	queueName = "testQueue"
	rabbitURL = "amqp://localhost:33192"
)

func StartRabbitMQContainerWithPort(port int) error {
	portStr := strconv.Itoa(port)
	cmdStatement := fmt.Sprintf("docker run --name rabbit-delay-test -p :%s:5672 -d rabbitmq", portStr)
	cmdParts := strings.Split(cmdStatement, " ")
	cmd := exec.Command(cmdParts[0], cmdParts[1:]...)
	return cmd.Run()
}

func RemoveRabbitMQContainer() error {
	cmdStatement := fmt.Sprintf("docker rm -f rabbit-delay-test")
	cmdParts := strings.Split(cmdStatement, " ")
	cmd := exec.Command(cmdParts[0], cmdParts[1:]...)
	return cmd.Run()
}

type ClientSuite struct {
	config Config
	suite.Suite
}

func TestClientSuite(t *testing.T) {
	RemoveRabbitMQContainer()
	StartRabbitMQContainerWithPort(33192)
	time.Sleep(5 * time.Second)

	s := &ClientSuite{}
	s.config = Config{
		URL:       rabbitURL,
		QueueName: queueName,
	}
	suite.Run(t, s)
}

func (s *ClientSuite) TearDownSuite() {
	RemoveRabbitMQContainer()
}

func (s *ClientSuite) TearDownTest() {
	client, _ := NewClient(s.config)
	require.NotNil(s.T(), client)
	ch, _ := client.Channel()
	require.NotNil(s.T(), ch)
	ch.QueuePurge(queueName, false)
}

func (s *ClientSuite) CheckQueue(ch Channel, numMessages int) {
	queue, err := ch.QueueInspect(queueName)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), numMessages, queue.Messages)
}

func (s *ClientSuite) ConsumeQueue(jobChan <-chan Job, errChan <-chan error, numMessages int) {
	jobCounter := 0
	errCounter := 0
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case j, ok := <-jobChan:
				if !ok {
					return
				}
				jobCounter++
				j.Ack(false)
			case _, ok := <-errChan:
				if !ok {
					return
				}
				errCounter++
			case <-time.After(1 * time.Second):
				return
			}
		}
	}()
	wg.Wait()
	assert.Equal(s.T(), numMessages, jobCounter)
	assert.Equal(s.T(), 0, errCounter)
}

func (s *ClientSuite) testConnReset(resetFunc func()) {
	client, err := NewClient(s.config)
	require.NoError(s.T(), err)
	ch, err := client.Channel()
	require.NoError(s.T(), err)
	require.NotNil(s.T(), ch)
	closeChan := client.GetCloseChan()

	publisher, err := NewPublisher(s.config)
	require.NoError(s.T(), err)

	consumer, err := NewConsumer(s.config)
	require.NoError(s.T(), err)
	jobChan, errChan, err := consumer.Consume()
	require.NoError(s.T(), err)

	numMessages := 50
	for i := 0; i < numMessages; i++ {
		err = publisher.Publish(Job{})
		require.NoError(s.T(), err)
	}
	time.Sleep(1 * time.Second)
	// this is because right when consumer starts, it caches 'defaultPrefetchCount' messages
	s.CheckQueue(ch, numMessages-defaultPrefetchCount)
	s.ConsumeQueue(jobChan, errChan, numMessages)
	s.CheckQueue(ch, 0)

	resetFunc()

	ch, err = client.Channel()
	require.NoError(s.T(), err)
	require.NotNil(s.T(), ch)
	select {
	case <-closeChan:
	default:
		require.Fail(s.T(), "closeChan did not receive notification")
	}

	for i := 0; i < numMessages; i++ {
		err = publisher.Publish(Job{})
		require.NoError(s.T(), err)
	}
	time.Sleep(1 * time.Second)
	// this is because right when consumer starts, it caches 'defaultPrefetchCount' messages
	s.CheckQueue(ch, numMessages-defaultPrefetchCount)
	s.ConsumeQueue(jobChan, errChan, numMessages)
	s.CheckQueue(ch, 0)
	err = consumer.Stop()
	require.NoError(s.T(), err)
}

func (s *ClientSuite) TestConnClose() {
	resetFunc := func() {
		CloseConn()
		time.Sleep(3 * time.Second)
	}
	s.testConnReset(resetFunc)
}

func (s *ClientSuite) TestRQTeardown() {

	resetFunc := func() {
		RemoveRabbitMQContainer()
		time.Sleep(1 * time.Second)

		badClient, err := NewClient(s.config)
		require.NoError(s.T(), err)
		require.NotNil(s.T(), badClient)
		ch, err := badClient.Channel()
		require.Error(s.T(), err)
		require.Nil(s.T(), ch)

		StartRabbitMQContainerWithPort(33192)
		time.Sleep(10 * time.Second)

	}

	s.testConnReset(resetFunc)
}

func (s *ClientSuite) TestAConnDownThenUp() {
	RemoveRabbitMQContainer()
	time.Sleep(1 * time.Second)

	client, err := NewClient(s.config)
	require.Error(s.T(), err)

	StartRabbitMQContainerWithPort(33192)
	time.Sleep(5 * time.Second)

	client, err = NewClient(s.config)
	require.NoError(s.T(), err)
	ch, err := client.Channel()
	require.NoError(s.T(), err)
	require.NotNil(s.T(), ch)
}
