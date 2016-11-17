package rabbitsmpp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type MultiConsumerSuite struct {
	suite.Suite
}

func TestMultiConsumerSuite(t *testing.T) {
	s := &MultiConsumerSuite{}
	suite.Run(t, s)
}

func (s *MultiConsumerSuite) SetupTest() {
}

func (s *MultiConsumerSuite) TearDownTest() {}

func (s *MultiConsumerSuite) TestStartStop() {

	multi := NewContainer()
	assert.Equal(s.T(), multi.ID(), "Container")
	j, e, err := multi.Consume()

	assert.NotNil(s.T(), j)
	assert.NotNil(s.T(), e)
	assert.Nil(s.T(), err)

	assert.Nil(s.T(), multi.Stop())

	_, ok := <-j
	assert.Equal(s.T(), ok, false)

	_, ok = <-e
	assert.Equal(s.T(), ok, false)

}

func getMockConsumer(id string, jobChan chan Job, errChan chan error) Consumer {

	mockConsumer := &MockConsumer{}
	mockConsumer.On("Consume").Return(jobChan, errChan, nil)
	mockConsumer.On("ID").Return(id)
	mockConsumer.On("Stop").Return(func() error {
		close(jobChan)
		close(errChan)
		//fmt.Println("Closed Channels")
		return nil
	})

	return mockConsumer
}

func (s *MultiConsumerSuite) TestAddConsumer() {

	multi := NewContainer()
	assert.Equal(s.T(), multi.ID(), "Container")
	mj, me, err := multi.Consume()

	assert.NotNil(s.T(), mj)
	assert.NotNil(s.T(), me)
	assert.Nil(s.T(), err)

	jobChan := make(chan Job)
	errChan := make(chan error)

	mockConsumer := getMockConsumer("mock1", jobChan, errChan)

	err = multi.AddRun(mockConsumer)
	assert.Nil(s.T(), err)

	// check if it's there
	assert.True(s.T(), multi.Exists(mockConsumer.ID()))

	// Add the same one again it should fail
	err = multi.AddRun(mockConsumer)
	assert.NotNil(s.T(), err)

	jobChan2 := make(chan Job)
	errChan2 := make(chan error)
	mockConsumer2 := getMockConsumer("mock2", jobChan2, errChan2)

	err = multi.AddRun(mockConsumer2)
	assert.Nil(s.T(), err)

	jobChan <- Job{}
	jobChan2 <- Job{}

	readJob := <-mj
	assert.NotNil(s.T(), readJob)

	readJob = <-mj
	assert.NotNil(s.T(), readJob)

	// The stop it
	mockConsumer.Stop()
	mockConsumer2.Stop()
	assert.Nil(s.T(), multi.Stop())

	_, ok := <-mj
	assert.Equal(s.T(), ok, false)

	_, ok = <-me
	assert.Equal(s.T(), ok, false)

}

func (s *MultiConsumerSuite) TestRemoveConsumer() {

	multi := NewContainer()
	assert.Equal(s.T(), multi.ID(), "Container")
	mj, me, err := multi.Consume()

	assert.NotNil(s.T(), mj)
	assert.NotNil(s.T(), me)
	assert.Nil(s.T(), err)

	jobChan := make(chan Job)
	errChan := make(chan error)

	mockConsumer := getMockConsumer("mock1", jobChan, errChan)

	err = multi.AddRun(mockConsumer)
	assert.Nil(s.T(), err)

	// Try to remove non existing one
	err = multi.RemoveStop("nonexisting")
	assert.NotNil(s.T(), err)

	jobChan <- Job{}
	readJob := <-mj
	assert.NotNil(s.T(), readJob)

	err = multi.RemoveStop(mockConsumer.ID())
	assert.Nil(s.T(), err)

	// it should be closed already
	_, ok := <-jobChan
	assert.Equal(s.T(), ok, false)

	assert.Nil(s.T(), multi.Stop())

	_, ok = <-mj
	assert.Equal(s.T(), ok, false)

	_, ok = <-me
	assert.Equal(s.T(), ok, false)

}
