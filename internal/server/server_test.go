package server

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"my-kv-store/internal/db"
	"my-kv-store/internal/middleware"
)

func setupTestServer(t *testing.T) (*Server, func()) {
	tmpDir, err := os.MkdirTemp("", "kv-server-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	opts := db.DefaultOptions(tmpDir)
	database, err := db.Open(opts)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to open database: %v", err)
	}

	limiter := middleware.NewIPRateLimiter(middleware.StrategySlidingWindowCounter, 100, 1*time.Minute)
	srv := NewServer(database, limiter)

	cleanup := func() {
		database.Close()
		os.RemoveAll(tmpDir)
	}

	return srv, cleanup
}

func TestServerCRUDOperations(t *testing.T) {
	srv, cleanup := setupTestServer(t)
	defer cleanup()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	// 1. Create (POST /api/kv)
	postBody, _ := json.Marshal(map[string]string{"key": "user:101", "value": "Alice Smith"})
	resp, err := http.Post(ts.URL+"/api/kv", "application/json", bytes.NewBuffer(postBody))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("Expected status 201 Created, got %d", resp.StatusCode)
	}

	// 2. Read (GET /api/kv/user:101)
	resp, err = http.Get(ts.URL + "/api/kv/user:101")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected status 200 OK, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	var getResp KVResponse
	json.Unmarshal(body, &getResp)
	if getResp.Value != "Alice Smith" {
		t.Fatalf("Expected 'Alice Smith', got '%s'", getResp.Value)
	}

	// 3. Update (PUT /api/kv/user:101)
	putBody, _ := json.Marshal(map[string]string{"value": "Alice Johnson"})
	req, _ := http.NewRequest("PUT", ts.URL+"/api/kv/user:101", bytes.NewBuffer(putBody))
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected status 200 OK, got %d", resp.StatusCode)
	}

	// Verify Update
	resp, _ = http.Get(ts.URL + "/api/kv/user:101")
	body, _ = io.ReadAll(resp.Body)
	json.Unmarshal(body, &getResp)
	if getResp.Value != "Alice Johnson" {
		t.Fatalf("Expected updated value 'Alice Johnson', got '%s'", getResp.Value)
	}

	// 4. Delete (DELETE /api/kv/user:101)
	req, _ = http.NewRequest("DELETE", ts.URL+"/api/kv/user:101", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected status 200 OK, got %d", resp.StatusCode)
	}

	// 5. Read after Delete -> Should return 404
	resp, _ = http.Get(ts.URL + "/api/kv/user:101")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("Expected status 404 Not Found after delete, got %d", resp.StatusCode)
	}
}

func TestServerMetricsAndConfig(t *testing.T) {
	srv, cleanup := setupTestServer(t)
	defer cleanup()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	// GET /api/metrics
	resp, err := http.Get(ts.URL + "/api/metrics")
	if err != nil {
		t.Fatalf("GET /api/metrics failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected status 200 OK for metrics, got %d", resp.StatusCode)
	}

	// POST /api/ratelimit/config
	cfgBody, _ := json.Marshal(map[string]interface{}{
		"strategy":       "leaky_bucket",
		"limit":          5,
		"window_seconds": 10,
	})
	resp, err = http.Post(ts.URL+"/api/ratelimit/config", "application/json", bytes.NewBuffer(cfgBody))
	if err != nil {
		t.Fatalf("POST /api/ratelimit/config failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected status 200 OK for config update, got %d", resp.StatusCode)
	}
}
