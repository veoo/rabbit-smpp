package rabbitsmpp

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
	options := []ConsumeOption{}

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
