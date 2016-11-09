package rabbitsmpp

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_MarshalAttributes(t *testing.T) {
	m := map[string]string{"foo": "bar", "this": "that"}
	attr := NewAttributes(m)

	expect := `{"foo":"bar","this":"that"}`
	bytes, err := json.Marshal(attr)
	assert.NoError(t, err)
	assert.Equal(t, expect, string(bytes))

	attr = NewAttributes(map[string]string{})
	expect = `{}`
	bytes, err = json.Marshal(attr)
	assert.NoError(t, err)
	assert.Equal(t, expect, string(bytes))
}

func Test_UnmarshalAttributes(t *testing.T) {
	m := map[string]string{"foo": "bar", "this": "that"}
	expect := NewAttributes(m)
	jsonStr := []byte(`{"foo":"bar","this":"that"}`)
	got := &Attributes{}
	err := json.Unmarshal(jsonStr, got)
	assert.NoError(t, err)
	assert.Equal(t, expect, got)

	expect = NewAttributes(map[string]string{})
	jsonStr = []byte(`{}`)
	got = &Attributes{}
	err = json.Unmarshal(jsonStr, got)
	assert.NoError(t, err)
	assert.Equal(t, expect, got)

	jsonStr = []byte(`{some-garbage`)
	got = &Attributes{}
	err = json.Unmarshal(jsonStr, got)
	assert.Error(t, err)

	jsonStr = []byte(`{"foo":"bar","this":"that"}`)
	got = nil
	err = json.Unmarshal(jsonStr, got)
	assert.Error(t, err)
}
