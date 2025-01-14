package settings

import (
	"encoding/json"
	"os"
	"strconv"
	"sync"
)

type KeyValues struct {
	values map[string]string
	path   string
	sync.RWMutex
}

func OpenKeyValues(path string) (*KeyValues, error) {
	s := &KeyValues{
		values: make(map[string]string),
		path:   path,
	}

	var err error
	var bs []byte
	bs, err = os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return s, nil
	}

	if err = json.Unmarshal(bs, &s.values); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *KeyValues) Get(key string) (string, bool) {
	s.RLock()
	defer s.RUnlock()
	v, ok := s.values[key]
	return v, ok && v != ""
}

func (s *KeyValues) GetBool(key string) (bool, bool) {
	if val, ok := s.Get(key); ok {
		if ret, err := strconv.ParseBool(val); err == nil {
			return ret, true
		}
	}
	return false, false
}

func (s *KeyValues) GetInt64(key string) (int64, bool) {
	if val, ok := s.Get(key); ok {
		if ret, err := strconv.ParseInt(val, 10, 64); err == nil {
			return ret, true
		}
	}
	return 0, false
}

func (s *KeyValues) Del(key string) (prev string, ok bool) {
	s.Lock()
	defer s.Unlock()
	prev, ok = s.values[key]
	if ok {
		delete(s.values, key)
	}
	return
}

func (s *KeyValues) Walk(f func(key, value string) bool) {
	s.RLock()
	defer s.RUnlock()
	for k, v := range s.values {
		if !f(k, v) {
			break
		}
	}
}

func (s *KeyValues) Set(key, value string) (err error) {
	s.Lock()
	defer s.Unlock()
	prev, has := s.values[key]
	if (!has && value == "") || (has && prev == value) {
		return nil
	}
	defer func() {
		if err == nil {
			return
		}
		// Rollback settings
		if !has {
			delete(s.values, key)
		} else {
			s.values[key] = prev
		}
	}()
	if value == "" {
		delete(s.values, key)
	} else {
		s.values[key] = value
	}
	err = s.persist()
	return
}

func (s *KeyValues) SetBool(key string, value bool) (err error) {
	return s.Set(key, strconv.FormatBool(value))
}

func (s *KeyValues) SetInt64(key string, value int64) (err error) {
	return s.Set(key, strconv.FormatInt(value, 10))
}

func (s *KeyValues) persist() error {
	var err error
	var bs []byte
	bs, err = json.Marshal(s.values)
	if err != nil {
		return err
	}
	var tmpFilepath = s.path + ".tmp"
	if err = os.WriteFile(tmpFilepath, bs, 0666); err != nil {
		return err
	}
	if err = os.Rename(tmpFilepath, s.path); err != nil {
		return err
	}
	return nil
}
