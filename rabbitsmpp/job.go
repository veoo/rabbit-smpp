package rabbitsmpp

import (
	"encoding/json"
	"errors"

	"github.com/streadway/amqp"
	"github.com/veoo/go-smpp/smpp/pdu"
)

type Job struct {
	PDUs       []pdu.Body        `json:"pdus"`
	Attributes map[string]string `json:"attributes"`
	delivery   *amqp.Delivery    `json:"-"`
}

// Since pdu.Body is an interface, we need an special method to pass a concrete type
func (j *Job) UnmarshalJSON(b []byte) error {
	s := &struct {
		PDUs       []*pdu.Codec      `json:"pdus"`
		Attributes map[string]string `json:"attributes"`
	}{}

	err := json.Unmarshal(b, &s)
	if err != nil {
		return err
	}
	j.PDUs = make([]pdu.Body, len(s.PDUs))
	for i := range j.PDUs {
		j.PDUs[i] = s.PDUs[i]
	}
	j.Attributes = s.Attributes
	return nil
}

func (j *Job) Ack(multiple bool) error {
	if j.delivery == nil {
		return errors.New("Delivery not set in Job")
	}
	return j.delivery.Ack(multiple)
}

func (j *Job) Nack(multiple, requeue bool) error {
	if j.delivery == nil {
		return errors.New("Delivery not set in Job")
	}
	return j.delivery.Nack(multiple, requeue)
}

func (j *Job) Reject(multiple bool) error {
	if j.delivery == nil {
		return errors.New("Delivery not set in Job")
	}
	return j.delivery.Reject(multiple)
}
