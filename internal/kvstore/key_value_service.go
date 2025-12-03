package kvstore

import (
	"context"
	"fmt"
	"sync"
)

const (
	DELETE = iota
	UPDATE = iota
	PUT    = iota
	GET    = iota
)

type KeyValueCommand struct {
	commandType int
	key         string
	value       *string
	output      chan KeyValueOutput
}

type KeyValueOutput struct {
	success bool
	value   *string
	err     error
}

type KeyValueService struct {
	input    chan KeyValueCommand
	isActive bool
	close    context.CancelFunc
}

var (
	instance *KeyValueService
	once     sync.Once
)

func GetKeyValueService(ctx context.Context, close context.CancelFunc) *KeyValueService {
	once.Do(func() {
		input := make(chan KeyValueCommand)
		InitKeyValueStore(input, ctx)
		instance = &KeyValueService{input, true, close}
	})
	return instance
}

func (kvService *KeyValueService) Close() {
	kvService.isActive = false
	kvService.close()
}

func (kvService *KeyValueService) CheckActive() error {
	if kvService.isActive {
		return nil
	}
	return fmt.Errorf("KeyValueService has been closed")
}

func (kvService *KeyValueService) Set(key string, value string) (*string, error) {
	if err := kvService.CheckActive(); err != nil {
		return nil, err
	}
	outputCh := make(chan KeyValueOutput)
	command := KeyValueCommand{PUT, key, &value, outputCh}
	kvService.input <- command
	res := <-outputCh

	return res.value, res.err
}

func (kvService *KeyValueService) Delete(key string) (*string, error) {
	if err := kvService.CheckActive(); err != nil {
		return nil, err
	}
	outputCh := make(chan KeyValueOutput)
	command := KeyValueCommand{DELETE, key, nil, outputCh}
	kvService.input <- command
	res := <-outputCh

	return res.value, res.err
}

func (kvService *KeyValueService) Get(key string) (*string, error) {
	if err := kvService.CheckActive(); err != nil {
		return nil, err
	}
	outputCh := make(chan KeyValueOutput)
	command := KeyValueCommand{GET, key, nil, outputCh}
	kvService.input <- command
	res := <-outputCh

	return res.value, res.err
}
