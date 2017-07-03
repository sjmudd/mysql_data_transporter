package main

import (
	"log"
	"sync"
)

type Cache struct {
	lock   sync.Mutex
	cache  map[string]string
	suffix string
}

func NewCache(suffix string) *Cache {
	if suffix == "" {
		log.Fatal("NewCache() suffix must not be empty")
	}
	return &Cache{
		cache:  make(map[string]string),
		suffix: suffix,
	}
}

func (rc *Cache) Set(name, value string) {
	rc.lock.Lock()
	defer rc.lock.Unlock()

	rc.cache[name] = value
}

func (rc *Cache) Get(name string) string {
	rc.lock.Lock()
	defer rc.lock.Unlock()

	var value string
	var found bool
	if value, found = rc.cache[name]; !found {
		value = name + rc.suffix
		rc.cache[name] = value
	}

	return value
}
