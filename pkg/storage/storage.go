package storage

import (
	"errors"
	"sync"
)

type Storage interface {
	Get(key string) (string, error)
	Set(key, value string) error
	Del(key string) error
}

func New() Storage {
	return &tempStorage{
		m:  make(map[string]string),
		mu: sync.Mutex{},
	}
}

type tempStorage struct {
	m  map[string]string
	mu sync.Mutex
}

func (s *tempStorage) Get(key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	v, exists := s.m[key]
	if !exists {
		return "", errors.New("key not exists")
	}
	return v, nil
}

func (s *tempStorage) Set(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.m[key] = value
	return nil
}

func (s *tempStorage) Del(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.m, key)
	return nil
}
