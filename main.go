package main

import (
	"context"
	"fmt"

	"blueis/models"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := models.GetKeyValueService(ctx)

	store.Set("hello", "world")
	val, _ := store.Get("hello")
	fmt.Println(*val)
	store.Delete("hello")
	val2, _ := store.Get("hello")
	fmt.Println(val2)
}
