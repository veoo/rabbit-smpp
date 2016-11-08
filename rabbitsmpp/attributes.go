package rabbitsmpp

import (
	"encoding/json"
	"sync"
)

type Attributes struct {
	mp    map[string]string
	mutex sync.RWMutex
}

func NewAttributes() *Attributes {
	return &Attributes{
		mp: map[string]string{},
	}
}

func (a *Attributes) Set(key string, value string) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	a.mp[key] = value
}

func (a *Attributes) Get(key string) string {
	a.mutex.RLock()
	defer a.mutex.RUnlock()
	return a.mp[key]
}

func (a *Attributes) Delete(key string) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	delete(a.mp, key)
}

func (a *Attributes) DeleteMulti(keys ...string) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	for _, key := range keys {
		delete(a.mp, key)
	}
}

func (a *Attributes) Keys() []string {
	a.mutex.RLock()
	defer a.mutex.RUnlock()
	keys := make([]string, len(a.mp))
	i := 0
	for key := range a.mp {
		keys[i] = key
		i++
	}
	return keys
}

func (a *Attributes) MarshalJSON() ([]byte, error) {
	a.mutex.RLock()
	defer a.mutex.RUnlock()
	return json.Marshal(a.mp)
}

func (a *Attributes) UnmarshalJSON(b []byte) error {
	m := map[string]string{}
	err := json.Unmarshal(b, &m)
	if err != nil {
		return err
	}
	a.mp = m
	return nil
}
