package main

import (
	"blueis/internal/kvstore"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type setRequest struct {
	Value string `json:"value"`
}

type response struct {
	Success bool    `json:"success"`
	Value   *string `json:"value,omitempty"`
	Error   string  `json:"error,omitempty"`
}

func main() {
	// Root context for the KV store
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kv := kvstore.GetKeyValueService(ctx, cancel)

	mux := http.NewServeMux()
	mux.HandleFunc("/kv", func(w http.ResponseWriter, r *http.Request) {
		handleKV(w, r, kv)
	})

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	// Start HTTP server
	go func() {
		log.Printf("HTTP server listening on %s\n", server.Addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Graceful shutdown on Ctrl+C / SIGTERM
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	<-stop
	log.Println("Shutting down server...")

	// Close KV service (cancels its context)
	kv.Close()

	ctxShutdown, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()

	if err := server.Shutdown(ctxShutdown); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exited gracefully")
}

func handleKV(w http.ResponseWriter, r *http.Request, kv *kvstore.KeyValueService) {
	w.Header().Set("Content-Type", "application/json")

	key := r.URL.Query().Get("key")
	if key == "" {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(response{
			Success: false,
			Error:   "missing 'key' query parameter",
		})
		return
	}

	switch r.Method {
	case http.MethodGet:
		handleGet(w, kv, key)
	case http.MethodPost, http.MethodPut:
		handleSet(w, r, kv, key)
	case http.MethodDelete:
		handleDelete(w, kv, key)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(response{
			Success: false,
			Error:   "method not allowed",
		})
	}
}

func handleGet(w http.ResponseWriter, kv *kvstore.KeyValueService, key string) {
	val, err := kv.Get(key)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(response{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	_ = json.NewEncoder(w).Encode(response{
		Success: true,
		Value:   val,
	})
}

func handleSet(w http.ResponseWriter, r *http.Request, kv *kvstore.KeyValueService, key string) {
	var req setRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(response{
			Success: false,
			Error:   "invalid JSON body",
		})
		return
	}

	val, err := kv.Set(key, req.Value)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(response{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	_ = json.NewEncoder(w).Encode(response{
		Success: true,
		Value:   val,
	})
}

func handleDelete(w http.ResponseWriter, kv *kvstore.KeyValueService, key string) {
	val, err := kv.Delete(key)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(response{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	_ = json.NewEncoder(w).Encode(response{
		Success: true,
		Value:   val, // may be nil if key didn't exist
	})
}
