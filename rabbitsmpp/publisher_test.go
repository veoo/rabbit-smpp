package rabbitsmpp

import (
	"errors"
	"testing"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestPublisherSucc(t *testing.T) {
	queueName := "mockQueue"
	mockClient := &MockClient{}
	mockClient.On("Config").Return(Config{QueueName: queueName})

	mockChannel := &MockChannel{}
	mockChannel.On("QueueDeclare", queueName, true, false, false, false, mock.Anything).Return(amqp.Queue{}, nil)

	mockClient.On("Channel").Return(mockChannel, nil)

	publisher, err := newPublisherWithClientFactory(func() (Client, error) {
		return mockClient, nil
	})

	assert.NoError(t, err)
	assert.NotNil(t, publisher)

	mockChannel.On("Publish", "", queueName, false, false, mock.Anything).Return(nil).Once()
	m := map[string]string{"systemID": "mockID"}
	err = publisher.Publish(Job{
		Attributes: NewAttributes(m),
	})
	assert.NoError(t, err)
	mockChannel.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestPublisherFail(t *testing.T) {
	mockClient := &MockClient{}
	queueName := "mockQueue"
	mockClient.On("Close").Return(nil)
	mockClient.On("Config").Return(Config{QueueName: queueName})

	mockChannel := &MockChannel{}
	mockChannel.On("Close").Return(nil)
	mockChannel.On("QueueDeclare", queueName, true, false, false, false, mock.Anything).Return(amqp.Queue{}, errors.New("some error"))
	mockClient.On("Channel").Return(mockChannel, nil)

	publisher, err := newPublisherWithClientFactory(func() (Client, error) {
		return mockClient, nil
	})

	assert.Error(t, err)
	assert.Nil(t, publisher)
	mockChannel.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestPublisherRetrySucc(t *testing.T) {
	queueName := "mockQueue"

	mockChannel := &MockChannel{}
	mockChannel.On("QueueDeclare", queueName, true, false, false, false, mock.Anything).Return(amqp.Queue{}, nil)

	mockClient := &MockClient{}
	mockClient.On("Config").Return(Config{QueueName: queueName})
	mockClient.On("Channel").Return(mockChannel, nil)

	newMockChannel := &MockChannel{}
	newMockChannel.On("QueueDeclare", queueName, true, false, false, false, mock.Anything).Return(amqp.Queue{}, nil)
	mockChannel.On("Publish", "", queueName, false, false, mock.Anything).Return(errors.New("conn closed")).Once()
	newMockChannel.On("Publish", "", queueName, false, false, mock.Anything).Return(nil).Once()

	newMockClient := &MockClient{}
	newMockClient.On("Channel").Return(newMockChannel, nil).Once()

	try := 0
	publisher, err := newPublisherWithClientFactory(func() (Client, error) {
		if try == 0 {
			try++
			return mockClient, nil
		} else {
			try++
			return newMockClient, nil
		}
	})

	assert.NoError(t, err)
	assert.NotNil(t, publisher)

	m := map[string]string{"systemID": "mockID"}
	err = publisher.Publish(Job{
		Attributes: NewAttributes(m),
	})
	assert.NoError(t, err)
	assert.Equal(t, 2, try)

	mockClient.AssertExpectations(t)
	newMockClient.AssertExpectations(t)
	mockChannel.AssertExpectations(t)
	newMockChannel.AssertExpectations(t)
}

func TestPublisherRetryFail(t *testing.T) {
	queueName := "mockQueue"

	mockClient := &MockClient{}
	mockClient.On("Config").Return(Config{QueueName: queueName})

	mockChannel := &MockChannel{}
	mockChannel.On("QueueDeclare", queueName, true, false, false, false, mock.Anything).Return(amqp.Queue{}, nil)

	mockClient.On("Channel").Return(mockChannel, nil)

	try := 0
	publisher, err := newPublisherWithClientFactory(func() (Client, error) {
		if try == 0 {
			try++
			return mockClient, nil
		} else {
			try++
			return nil, errors.New("error getting client")
		}
	})

	assert.NoError(t, err)
	assert.NotNil(t, publisher)

	mockChannel.On("Publish", "", queueName, false, false, mock.Anything).Return(errors.New("conn closed")).Once()

	m := map[string]string{"systemID": "mockID"}
	err = publisher.Publish(Job{
		Attributes: NewAttributes(m),
	})
	assert.Equal(t, 2, try)
	assert.Error(t, err)

	mockClient.AssertExpectations(t)
	mockChannel.AssertExpectations(t)
}

func TestDelayedPublisherSucc(t *testing.T) {
	delayExchange := "mockExchange"
	delayMS := 1000
	queueName := "mockQueue"

	mockClient := &MockClient{}
	mockClient.On("Config").Return(Config{QueueName: queueName})

	mockChannel := &MockChannel{}
	mockChannel.On("QueueDeclare", queueName, true, false, false, false, mock.Anything).Return(amqp.Queue{
		Name: "mockQueue",
	}, nil)
	mockChannel.On("ExchangeDeclare", delayExchange, "x-delayed-message",
		true,
		false,
		false,
		true,
		amqp.Table{
			"x-delayed-type": "direct",
		}).Return(nil)

	mockChannel.On("QueueBind", queueName, queueName, delayExchange, false, mock.Anything).Return(nil)

	mockClient.On("Channel").Return(mockChannel, nil)
	factory := func() (Client, error) {
		return mockClient, nil
	}
	publisher, err := newDelayedPublisherWithClientFactory(factory, delayExchange, delayMS)

	assert.NoError(t, err)
	assert.NotNil(t, publisher)

	mockChannel.On("Publish", delayExchange, queueName, false, false, mock.Anything).Return(nil).Once()
	m := map[string]string{"systemID": "mockID"}
	err = publisher.Publish(Job{
		Attributes: NewAttributes(m),
	})
	assert.NoError(t, err)

	mockChannel.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestDelayedPublisherFail(t *testing.T) {
	delayExchange := "mockExchange"
	delayMS := 1000
	queueName := "mockQueue"

	mockClient := &MockClient{}
	mockClient.On("Close").Return(nil)
	mockClient.On("Config").Return(Config{QueueName: queueName})

	mockChannel := &MockChannel{}
	mockChannel.On("Close").Return(nil)
	mockChannel.On("ExchangeDeclare", delayExchange, "x-delayed-message", true, false, false, true, mock.Anything).Return(errors.New("some error"))
	mockClient.On("Channel").Return(mockChannel, nil)
	factory := func() (Client, error) {
		return mockClient, nil
	}
	publisher, err := newDelayedPublisherWithClientFactory(factory, delayExchange, delayMS)

	assert.Error(t, err)
	assert.Nil(t, publisher)

	mockChannel.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestDelayedPublisherRetrySucc(t *testing.T) {
	delayExchange := "mockExchange"
	delayMS := 1000
	queueName := "mockQueue"

	mockClient := &MockClient{}
	mockClient.On("Config").Return(Config{QueueName: queueName})

	mockChannel := &MockChannel{}
	mockChannel.On("QueueDeclare", queueName, true, false, false, false, mock.Anything).Return(amqp.Queue{
		Name: "mockQueue",
	}, nil)
	mockChannel.On("ExchangeDeclare", delayExchange, "x-delayed-message",
		true,
		false,
		false,
		true,
		amqp.Table{
			"x-delayed-type": "direct",
		}).Return(nil)

	mockChannel.On("QueueBind", queueName, queueName, delayExchange, false, mock.Anything).Return(nil)

	mockClient.On("Channel").Return(mockChannel, nil)

	newMockChannel := &MockChannel{}
	newMockChannel.On("QueueDeclare", queueName, true, false, false, false, mock.Anything).Return(amqp.Queue{
		Name: queueName,
	}, nil)
	newMockChannel.On("ExchangeDeclare", delayExchange, "x-delayed-message",
		true,
		false,
		false,
		true,
		amqp.Table{
			"x-delayed-type": "direct",
		}).Return(nil)

	newMockChannel.On("QueueBind", queueName, queueName, delayExchange, false, mock.Anything).Return(nil)

	newMockClient := &MockClient{}
	newMockClient.On("Channel").Return(newMockChannel, nil).Once()

	mockChannel.On("Publish", delayExchange, queueName, false, false, mock.Anything).Return(errors.New("conn closed")).Once()
	newMockChannel.On("Publish", delayExchange, queueName, false, false, mock.Anything).Return(nil).Once()
	try := 0
	factory := func() (Client, error) {
		if try == 0 {
			try++
			return mockClient, nil
		} else {
			try++
			return newMockClient, nil
		}
	}
	publisher, err := newDelayedPublisherWithClientFactory(factory, delayExchange, delayMS)

	assert.NoError(t, err)
	assert.NotNil(t, publisher)

	m := map[string]string{"systemID": "mockID"}
	err = publisher.Publish(Job{
		Attributes: NewAttributes(m),
	})
	assert.NoError(t, err)
	assert.Equal(t, 2, try)

	mockClient.AssertExpectations(t)
	newMockClient.AssertExpectations(t)
	mockChannel.AssertExpectations(t)
	newMockChannel.AssertExpectations(t)
}

func TestDelayedPublisherRetryFail(t *testing.T) {
	delayExchange := "mockExchange"
	delayMS := 1000
	queueName := "mockQueue"

	mockClient := &MockClient{}
	mockClient.On("Config").Return(Config{QueueName: queueName})

	mockChannel := &MockChannel{}
	mockChannel.On("QueueDeclare", queueName, true, false, false, false, mock.Anything).Return(amqp.Queue{
		Name: queueName,
	}, nil)
	mockChannel.On("ExchangeDeclare", delayExchange, "x-delayed-message",
		true,
		false,
		false,
		true,
		amqp.Table{
			"x-delayed-type": "direct",
		}).Return(nil)

	mockChannel.On("QueueBind", queueName, queueName, delayExchange, false, mock.Anything).Return(nil)

	mockClient.On("Channel").Return(mockChannel, nil)
	try := 0
	factory := func() (Client, error) {
		if try == 0 {
			try++
			return mockClient, nil
		} else {
			try++
			return nil, errors.New("some error")
		}
	}
	publisher, err := newDelayedPublisherWithClientFactory(factory, delayExchange, delayMS)

	assert.NoError(t, err)
	assert.NotNil(t, publisher)

	mockChannel.On("Publish", delayExchange, queueName, false, false, mock.Anything).Return(errors.New("conn closed")).Once()

	m := map[string]string{"systemID": "mockID"}
	err = publisher.Publish(Job{
		Attributes: NewAttributes(m),
	})
	assert.Error(t, err)
	assert.Equal(t, 2, try)

	mockClient.AssertExpectations(t)
	mockChannel.AssertExpectations(t)
}
