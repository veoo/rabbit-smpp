package rabbitsmpp

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_MarshalAttributes(t *testing.T) {
	attr := NewAttributes()
	attr.Set("foo", "bar")
	attr.Set("this", "that")

	expect := `{"foo":"bar","this":"that"}`
	bytes, err := json.Marshal(attr)
	assert.NoError(t, err)
	assert.Equal(t, expect, string(bytes))

	attr = NewAttributes()
	expect = `{}`
	bytes, err = json.Marshal(attr)
	assert.NoError(t, err)
	assert.Equal(t, expect, string(bytes))
}

func Test_UnmarshalAttributes(t *testing.T) {
	jsonStr := []byte(`{"foo":"bar","this":"that"}`)
	got := &Attributes{}
	err := json.Unmarshal(jsonStr, got)
	assert.NoError(t, err)

	expect := NewAttributes()
	expect.Set("foo", "bar")
	expect.Set("this", "that")
	assert.Equal(t, expect, got)

	jsonStr = []byte(`{}`)
	got = &Attributes{}
	err = json.Unmarshal(jsonStr, got)
	assert.NoError(t, err)

	expect = NewAttributes()
	assert.Equal(t, expect, got)

	jsonStr = []byte(`{some-garbage`)
	got = &Attributes{}
	err = json.Unmarshal(jsonStr, got)
	assert.Error(t, err)
}
