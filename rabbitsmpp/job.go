package rabbitsmpp

import (
	"encoding/json"

	"github.com/veoo/go-smpp/smpp/pdu"
)

type Job struct {
	PDU        pdu.Body          `json:"pdu"`
	Attributes map[string]string `json:"attributes"`
}

// Since pdu.Body is an interface, we need an special method to pass a concrete type
func (j *Job) UnmarshalJSON(b []byte) error {
	s := &struct {
		PDU        *pdu.Codec        `json:"pdu"`
		Attributes map[string]string `json:"attributes"`
	}{}

	err := json.Unmarshal(b, &s)
	if err != nil {
		return err
	}
	j.PDU = s.PDU
	j.Attributes = s.Attributes
	return nil
}
