package rabbitsmpp

import (
	"errors"
	"testing"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestPublisherSucc(t *testing.T) {

	client := &MockPublisherClient{}
	client.On("Bind").Return(make(chan *amqp.Error), nil)
	client.On("QueueName").Return("mockQueue")

	mockChannel := &MockChannel{}
	mockChannel.On("QueueDeclare", "mockQueue", true, false, false, false, mock.Anything).Return(amqp.Queue{}, nil)

	client.On("Channel").Return(mockChannel, nil)

	publisher, err := newPublisherWithClient(client)

	assert.NoError(t, err)
	assert.NotNil(t, publisher)

	client.AssertNumberOfCalls(t, "Bind", 1)

	mockChannel.On("Publish", "", "mockQueue", false, false, mock.Anything).Return(nil)
	attr := NewAttributes()
	attr.Set("systemID", "mockID")
	err = publisher.Publish(Job{
		Attributes: attr,
	})
	assert.NoError(t, err)
	mockChannel.AssertNumberOfCalls(t, "Publish", 1)

}

func TestPublisherFail(t *testing.T) {
	client := &MockPublisherClient{}
	client.On("Bind").Return(nil, errors.New("bind"))

	publisher, err := newPublisherWithClient(client)

	assert.Error(t, err)
	assert.Nil(t, publisher)

	client.AssertNumberOfCalls(t, "Bind", 1)

}

func TestPublisherRetrySucc(t *testing.T) {

	client := &MockPublisherClient{}
	client.On("Bind").Return(make(chan *amqp.Error), nil)
	client.On("QueueName").Return("mockQueue")

	mockChannel := &MockChannel{}
	mockChannel.On("QueueDeclare", "mockQueue", true, false, false, false, mock.Anything).Return(amqp.Queue{}, nil)

	client.On("Channel").Return(mockChannel, nil)

	publisher, err := newPublisherWithClient(client)

	assert.NoError(t, err)
	assert.NotNil(t, publisher)

	client.AssertNumberOfCalls(t, "Bind", 1)

	newMockPublisherClient := &MockPublisherClient{}
	newMockPublisherClient.On("QueueName").Return("mockQueue")
	newMockChannel := &MockChannel{}
	newMockChannel.On("QueueDeclare", "mockQueue", true, false, false, false, mock.Anything).Return(amqp.Queue{}, nil)
	newMockPublisherClient.On("Channel").Return(newMockChannel, nil)

	client.On("ReBind").Return(newMockPublisherClient, nil, nil)

	mockChannel.On("Publish", "", "mockQueue", false, false, mock.Anything).Return(errors.New("conn closed"))
	newMockChannel.On("Publish", "", "mockQueue", false, false, mock.Anything).Return(nil)

	attr := NewAttributes()
	attr.Set("systemID", "mockID")
	err = publisher.Publish(Job{
		Attributes: attr,
	})
	assert.NoError(t, err)

	client.AssertNumberOfCalls(t, "ReBind", 1)
	newMockPublisherClient.AssertNumberOfCalls(t, "Channel", 1)
	mockChannel.AssertNumberOfCalls(t, "Publish", 1)
	newMockChannel.AssertNumberOfCalls(t, "Publish", 1)

}

func TestPublisherRetryFail(t *testing.T) {

	client := &MockPublisherClient{}
	client.On("Bind").Return(make(chan *amqp.Error), nil)
	client.On("QueueName").Return("mockQueue")

	mockChannel := &MockChannel{}
	mockChannel.On("QueueDeclare", "mockQueue", true, false, false, false, mock.Anything).Return(amqp.Queue{}, nil)

	client.On("Channel").Return(mockChannel, nil)

	publisher, err := newPublisherWithClient(client)

	assert.NoError(t, err)
	assert.NotNil(t, publisher)

	client.AssertNumberOfCalls(t, "Bind", 1)

	mockChannel.On("Publish", "", "mockQueue", false, false, mock.Anything).Return(errors.New("conn closed"))
	client.On("ReBind").Return(nil, nil, errors.New("bind retry fail"))
	attr := NewAttributes()
	attr.Set("systemID", "mockID")
	err = publisher.Publish(Job{
		Attributes: attr,
	})
	assert.Error(t, err)

	client.AssertNumberOfCalls(t, "ReBind", 1)
	mockChannel.AssertNumberOfCalls(t, "Publish", 1)

}

func TestDelayedPublisherSucc(t *testing.T) {
	delayExchange := "mockEchange"
	delayMS := 1000

	client := &MockPublisherClient{}
	client.On("Bind").Return(make(chan *amqp.Error), nil)
	client.On("QueueName").Return("mockQueue")

	mockChannel := &MockChannel{}
	mockChannel.On("QueueDeclare", "mockQueue", true, false, false, false, mock.Anything).Return(amqp.Queue{
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

	mockChannel.On("QueueBind", "mockQueue", "mockQueue", delayExchange, false, mock.Anything).Return(nil)

	client.On("Channel").Return(mockChannel, nil)

	publisher, err := newDelayedPublisherWithClient(client, delayExchange, delayMS)

	assert.NoError(t, err)
	assert.NotNil(t, publisher)

	client.AssertNumberOfCalls(t, "Bind", 1)

	mockChannel.On("Publish", delayExchange, "mockQueue", false, false, mock.Anything).Return(nil)
	attr := NewAttributes()
	attr.Set("systemID", "mockID")
	err = publisher.Publish(Job{
		Attributes: attr,
	})
	assert.NoError(t, err)
	mockChannel.AssertNumberOfCalls(t, "Publish", 1)

}

func TestDelayedPublisherFail(t *testing.T) {
	delayExchange := "mockEchange"
	delayMS := 1000

	client := &MockPublisherClient{}
	client.On("Bind").Return(nil, errors.New("bind"))

	publisher, err := newDelayedPublisherWithClient(client, delayExchange, delayMS)

	assert.Error(t, err)
	assert.Nil(t, publisher)

	client.AssertNumberOfCalls(t, "Bind", 1)

}

func TestDelayedPublisherRetrySucc(t *testing.T) {
	delayExchange := "mockEchange"
	delayMS := 1000

	client := &MockPublisherClient{}
	client.On("Bind").Return(make(chan *amqp.Error), nil)
	client.On("QueueName").Return("mockQueue")

	mockChannel := &MockChannel{}
	mockChannel.On("QueueDeclare", "mockQueue", true, false, false, false, mock.Anything).Return(amqp.Queue{
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

	mockChannel.On("QueueBind", "mockQueue", "mockQueue", delayExchange, false, mock.Anything).Return(nil)

	client.On("Channel").Return(mockChannel, nil)

	publisher, err := newDelayedPublisherWithClient(client, delayExchange, delayMS)

	assert.NoError(t, err)
	assert.NotNil(t, publisher)

	client.AssertNumberOfCalls(t, "Bind", 1)

	newMockPublisherClient := &MockPublisherClient{}
	newMockPublisherClient.On("QueueName").Return("mockQueue")
	newMockChannel := &MockChannel{}
	newMockChannel.On("QueueDeclare", "mockQueue", true, false, false, false, mock.Anything).Return(amqp.Queue{
		Name: "mockQueue",
	}, nil)
	newMockChannel.On("ExchangeDeclare", delayExchange, "x-delayed-message",
		true,
		false,
		false,
		true,
		amqp.Table{
			"x-delayed-type": "direct",
		}).Return(nil)

	newMockChannel.On("QueueBind", "mockQueue", "mockQueue", delayExchange, false, mock.Anything).Return(nil)

	newMockPublisherClient.On("Channel").Return(newMockChannel, nil)

	client.On("ReBind").Return(newMockPublisherClient, nil, nil)

	mockChannel.On("Publish", delayExchange, "mockQueue", false, false, mock.Anything).Return(errors.New("conn closed"))
	newMockChannel.On("Publish", delayExchange, "mockQueue", false, false, mock.Anything).Return(nil)

	attr := NewAttributes()
	attr.Set("systemID", "mockID")
	err = publisher.Publish(Job{
		Attributes: attr,
	})
	assert.NoError(t, err)

	client.AssertNumberOfCalls(t, "ReBind", 1)
	newMockPublisherClient.AssertNumberOfCalls(t, "Channel", 1)
	mockChannel.AssertNumberOfCalls(t, "Publish", 1)
	newMockChannel.AssertNumberOfCalls(t, "Publish", 1)

}

func TestDelayedPublisherRetryFail(t *testing.T) {
	delayExchange := "mockEchange"
	delayMS := 1000

	client := &MockPublisherClient{}
	client.On("Bind").Return(make(chan *amqp.Error), nil)
	client.On("QueueName").Return("mockQueue")

	mockChannel := &MockChannel{}
	mockChannel.On("QueueDeclare", "mockQueue", true, false, false, false, mock.Anything).Return(amqp.Queue{
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

	mockChannel.On("QueueBind", "mockQueue", "mockQueue", delayExchange, false, mock.Anything).Return(nil)

	client.On("Channel").Return(mockChannel, nil)

	publisher, err := newDelayedPublisherWithClient(client, delayExchange, delayMS)

	assert.NoError(t, err)
	assert.NotNil(t, publisher)

	client.AssertNumberOfCalls(t, "Bind", 1)

	client.On("ReBind").Return(nil, nil, errors.New("rebind failed"))
	mockChannel.On("Publish", delayExchange, "mockQueue", false, false, mock.Anything).Return(errors.New("conn closed"))

	attr := NewAttributes()
	attr.Set("systemID", "mockID")
	err = publisher.Publish(Job{
		Attributes: attr,
	})
	assert.Error(t, err)

	client.AssertNumberOfCalls(t, "ReBind", 1)
	mockChannel.AssertNumberOfCalls(t, "Publish", 1)

}
