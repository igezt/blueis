package kvstore

import (
	"context"
	"fmt"
)

type KeyValueStore struct {
	store map[string]string
}

func InitKeyValueStore(input chan KeyValueCommand, ctx context.Context) {
	store := KeyValueStore{make(map[string]string)}
	go store.Start(input, ctx)
}

func (kvStore KeyValueStore) Start(input chan KeyValueCommand, ctx context.Context) {
	for {
		select {
		case msg := <-input:
			kvStore.ProcessCommand(msg)
		case <-ctx.Done():
			fmt.Println("Key value store shutting down")
			return
		}
	}
}

func (kvStore KeyValueStore) ProcessCommand(command KeyValueCommand) {

	switch command.commandType {
	case PUT:
		kvStore.ProcessPutCommand(command)
	case GET:
		kvStore.ProcessGetCommand(command)
	case DELETE:
		kvStore.ProcessDeleteCommand(command)
	default:
		command.output <- KeyValueOutput{false, nil, fmt.Errorf("command type %s not found", GetCommandTypeString(command.commandType))}
	}
}

func (kvStore KeyValueStore) ProcessPutCommand(command KeyValueCommand) {
	key := command.key
	val := command.value
	if val == nil {
		command.output <- KeyValueOutput{false, nil, fmt.Errorf("value given was nil for put command")}
	} else {
		kvStore.store[key] = *val
		command.output <- KeyValueOutput{true, val, nil}
	}
}

func (kvStore KeyValueStore) ProcessGetCommand(command KeyValueCommand) {
	key := command.key
	if value, ok := kvStore.store[key]; ok {
		command.output <- KeyValueOutput{true, &value, nil}
	} else {
		command.output <- KeyValueOutput{false, nil, fmt.Errorf("key %s does not exist in the store", key)}
	}
}

func (kvStore KeyValueStore) ProcessDeleteCommand(command KeyValueCommand) {
	key := command.key
	if value, ok := kvStore.store[key]; ok {
		delete(kvStore.store, key)
		command.output <- KeyValueOutput{true, &value, nil}
	} else {
		command.output <- KeyValueOutput{true, nil, nil}
	}
}

func GetCommandTypeString(commandType int) string {
	switch commandType {
	case PUT:
		return "PUT"
	case DELETE:
		return "DELETE"
	case GET:
		return "GET"
	}
	return "UNKNOWN"
}
