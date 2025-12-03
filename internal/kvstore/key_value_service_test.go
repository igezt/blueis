package kvstore

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
)

func deref(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}

func newTestKeyValueService(t *testing.T) *KeyValueService {
	t.Helper()

	// reset the singleton for a clean state per test
	instance = nil
	once = sync.Once{}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	return GetKeyValueService(ctx, cancel)
}

func TestSetAndGet_ReturnsSameValue(t *testing.T) {
	store := newTestKeyValueService(t)

	key := "foo"
	value := "bar"

	setVal, err := store.Set(key, value)
	if err != nil {
		t.Fatalf("Set(%q, %q) returned error: %v", key, value, err)
	}
	if setVal == nil {
		t.Fatalf("Set(%q, %q) returned nil value", key, value)
	}
	if *setVal != value {
		t.Fatalf("Set(%q, %q) = %q, want %q", key, value, *setVal, value)
	}

	got, err := store.Get(key)
	if err != nil {
		t.Fatalf("Get(%q) returned error: %v", key, err)
	}
	if got == nil {
		t.Fatalf("Get(%q) returned nil value", key)
	}
	if *got != value {
		t.Fatalf("Get(%q) = %q, want %q", key, *got, value)
	}
}

func TestGet_MissingKey_ReturnsError(t *testing.T) {
	store := newTestKeyValueService(t)

	key := "does-not-exist"

	got, err := store.Get(key)
	if err == nil {
		t.Fatalf("Get(%q) expected error for missing key, got nil", key)
	}
	if got != nil {
		t.Fatalf("Get(%q) expected nil value for missing key, got %q", key, *got)
	}
}

func TestSet_OverwritesExistingValue(t *testing.T) {
	store := newTestKeyValueService(t)

	key := "foo"
	first := "bar"
	second := "baz"

	if _, err := store.Set(key, first); err != nil {
		t.Fatalf("Set(%q, %q) returned error: %v", key, first, err)
	}

	if _, err := store.Set(key, second); err != nil {
		t.Fatalf("Set(%q, %q) returned error: %v", key, second, err)
	}

	got, err := store.Get(key)
	if err != nil {
		t.Fatalf("Get(%q) returned error: %v", key, err)
	}
	if got == nil || *got != second {
		t.Fatalf("Get(%q) = %v, want %q", key, deref(got), second)
	}
}

func TestDelete_ExistingKey_RemovesAndReturnsValue(t *testing.T) {
	store := newTestKeyValueService(t)

	key := "foo"
	value := "bar"

	if _, err := store.Set(key, value); err != nil {
		t.Fatalf("Set(%q, %q) returned error: %v", key, value, err)
	}

	deleted, err := store.Delete(key)
	if err != nil {
		t.Fatalf("Delete(%q) returned error: %v", key, err)
	}
	if deleted == nil || *deleted != value {
		t.Fatalf("Delete(%q) = %v, want %q", key, deref(deleted), value)
	}

	// ensure it's gone
	got, err := store.Get(key)
	if err == nil {
		t.Fatalf("Get(%q) after Delete expected error, got nil", key)
	}
	if got != nil {
		t.Fatalf("Get(%q) after Delete expected nil value, got %q", key, *got)
	}
}

func TestDelete_MissingKey_SucceedsWithNilValue(t *testing.T) {
	store := newTestKeyValueService(t)

	key := "does-not-exist"

	deleted, err := store.Delete(key)
	if err != nil {
		t.Fatalf("Delete(%q) expected nil error for missing key, got %v", key, err)
	}
	if deleted != nil {
		t.Fatalf("Delete(%q) expected nil value for missing key, got %q", key, *deleted)
	}
}

func TestClose_PreventsFurtherOperations(t *testing.T) {
	store := newTestKeyValueService(t)

	store.Close()

	// all operations should now fail with CheckActive error
	if _, err := store.Set("k", "v"); err == nil {
		t.Fatalf("Set after Close() expected error, got nil")
	}

	if _, err := store.Get("k"); err == nil {
		t.Fatalf("Get after Close() expected error, got nil")
	}

	if _, err := store.Delete("k"); err == nil {
		t.Fatalf("Delete after Close() expected error, got nil")
	}
}

func TestGetCommandTypeString(t *testing.T) {
	tests := []struct {
		input int
		want  string
	}{
		{PUT, "PUT"},
		{DELETE, "DELETE"},
		{GET, "GET"},
		{999, "UNKNOWN"},
	}

	for _, tt := range tests {
		got := GetCommandTypeString(tt.input)
		if got != tt.want {
			t.Errorf("GetCommandTypeString(%d) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestConcurrentSetsAndGets(t *testing.T) {
	store := newTestKeyValueService(t)

	const numGoroutines = 50
	const keysPerGoroutine = 20

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := range numGoroutines {
		go func(id int) {
			defer wg.Done()
			for j := range keysPerGoroutine {
				key := fmt.Sprintf("k-%d-%d", id, j)
				val := fmt.Sprintf("v-%d-%d", id, j)

				if _, err := store.Set(key, val); err != nil {
					t.Errorf("goroutine %d: Set(%q, %q) returned error: %v", id, key, val, err)
					return
				}
			}
		}(i)
	}

	wg.Wait()

	for i := range numGoroutines {
		for j := range keysPerGoroutine {
			key := fmt.Sprintf("k-%d-%d", i, j)
			want := fmt.Sprintf("v-%d-%d", i, j)

			got, err := store.Get(key)
			if err != nil {
				t.Fatalf("Get(%q) returned error: %v", key, err)
			}
			if got == nil || *got != want {
				t.Fatalf("Get(%q) = %v, want %q", key, deref(got), want)
			}
		}
	}
}

func TestConcurrentSetSameKey(t *testing.T) {
	store := newTestKeyValueService(t)

	const numGoroutines = 100
	key := "shared-key"

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	values := make([]string, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		values[i] = fmt.Sprintf("value-%d", i)
	}

	// many goroutines writing different values to the same key
	for i := 0; i < numGoroutines; i++ {
		v := values[i]
		go func(val string) {
			defer wg.Done()
			if _, err := store.Set(key, val); err != nil {
				t.Errorf("Set(%q, %q) returned error: %v", key, val, err)
			}
		}(v)
	}

	wg.Wait()

	// final value must be one of the values we wrote, and no error
	got, err := store.Get(key)
	if err != nil {
		t.Fatalf("Get(%q) returned error: %v", key, err)
	}
	if got == nil {
		t.Fatalf("Get(%q) returned nil value", key)
	}

	final := *got
	found := slices.Contains(values, final)
	if !found {
		t.Fatalf("Final value %q for key %q was not one of the written values", final, key)
	}
}
