package rabbitsmpp

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const (
	queueName = "testQueue"
	rabbitURL = "amqp://localhost:33192"
)

func StartRabbitMQContainerWithPort(port int) error {
	portStr := strconv.Itoa(port)
	cmdStatement := fmt.Sprintf("docker run --name rabbit-delay-test -p :%s:5672 -d rq_delay_queue:latest", portStr)
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

func (s *ClientSuite) TestConnClose() {

	client, err := NewClient(s.config)
	require.NoError(s.T(), err)
	ch, err := client.Channel()
	require.NoError(s.T(), err)
	require.NotNil(s.T(), ch)
	closeChan := client.GetCloseChan()

	CloseConn()
	time.Sleep(3 * time.Second)

	ch, err = client.Channel()
	require.NoError(s.T(), err)
	require.NotNil(s.T(), ch)
	select {
	case <-closeChan:
	default:
		require.Fail(s.T(), "closeChan did not receive notification")
	}
}

func (s *ClientSuite) TestRQTeardown() {

	client, err := NewClient(s.config)
	require.NoError(s.T(), err)
	ch, err := client.Channel()
	require.NoError(s.T(), err)
	require.NotNil(s.T(), ch)
	closeChan := client.GetCloseChan()

	RemoveRabbitMQContainer()
	time.Sleep(1 * time.Second)

	StartRabbitMQContainerWithPort(33192)
	time.Sleep(5 * time.Second)

	ch, err = client.Channel()
	require.NoError(s.T(), err)
	require.NotNil(s.T(), ch)
	select {
	case <-closeChan:
	default:
		require.Fail(s.T(), "closeChan did not receive notification")
	}
}
