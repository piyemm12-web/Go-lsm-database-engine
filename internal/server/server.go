package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"my-kv-store/internal/db"
	"my-kv-store/internal/middleware"
)

type Server struct {
	db      *db.DB
	limiter *middleware.IPRateLimiter
	mux     *http.ServeMux
}

type KVRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type KVResponse struct {
	Key     string `json:"key,omitempty"`
	Value   string `json:"value,omitempty"`
	Message string `json:"message,omitempty"`
}

type ConfigRequest struct {
	Strategy      string `json:"strategy"`
	Limit         int64  `json:"limit"`
	WindowSeconds int64  `json:"window_seconds"`
}

func NewServer(database *db.DB, limiter *middleware.IPRateLimiter) *Server {
	s := &Server{
		db:      database,
		limiter: limiter,
		mux:     http.NewServeMux(),
	}

	s.routes()
	return s
}

func (s *Server) routes() {
	// API routes
	apiMux := http.NewServeMux()
	apiMux.HandleFunc("POST /api/kv", s.handleCreateUpdate)
	apiMux.HandleFunc("GET /api/kv", s.handleGetOrList)
	apiMux.HandleFunc("GET /api/kv/", s.handleGetByPath)
	apiMux.HandleFunc("PUT /api/kv/", s.handleUpdateByPath)
	apiMux.HandleFunc("DELETE /api/kv/", s.handleDeleteByPath)
	
	apiMux.HandleFunc("GET /api/metrics", s.handleMetrics)
	apiMux.HandleFunc("POST /api/ratelimit/config", s.handleConfigRateLimiter)
	apiMux.HandleFunc("GET /api/health", s.handleHealth)

	// Wrap API routes with RateLimiter middleware
	rateLimitedAPI := s.limiter.Handler(apiMux)

	s.mux.Handle("/api/", rateLimitedAPI)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) RegisterStatic(fs http.Handler) {
	s.mux.Handle("/", fs)
}

// ---------------------------------------------------------
// CRUD Handlers
// ---------------------------------------------------------

func (s *Server) handleCreateUpdate(w http.ResponseWriter, r *http.Request) {
	var req KVRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid JSON body"})
		return
	}

	if strings.TrimSpace(req.Key) == "" {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "Key cannot be empty"})
		return
	}

	if err := s.db.Put([]byte(req.Key), []byte(req.Value)); err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("Failed to store key: %v", err)})
		return
	}

	respondJSON(w, http.StatusCreated, KVResponse{
		Key:     req.Key,
		Value:   req.Value,
		Message: "Key-value pair saved successfully",
	})
}

func (s *Server) handleGetOrList(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "Missing key query parameter"})
		return
	}
	s.getAndRespond(w, key)
}

func (s *Server) handleGetByPath(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/api/kv/")
	if key == "" {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "Key parameter required in URL path"})
		return
	}
	s.getAndRespond(w, key)
}

func (s *Server) getAndRespond(w http.ResponseWriter, key string) {
	val, err := s.db.Get([]byte(key))
	if err != nil {
		if errors.Is(err, db.ErrKeyNotFound) {
			respondJSON(w, http.StatusNotFound, map[string]string{"error": "Key not found", "key": key})
			return
		}
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("Database error: %v", err)})
		return
	}

	respondJSON(w, http.StatusOK, KVResponse{
		Key:   key,
		Value: string(val),
	})
}

func (s *Server) handleUpdateByPath(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/api/kv/")
	if key == "" {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "Key parameter required in URL path"})
		return
	}

	var req KVRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid JSON body"})
		return
	}

	if err := s.db.Put([]byte(key), []byte(req.Value)); err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("Failed to update key: %v", err)})
		return
	}

	respondJSON(w, http.StatusOK, KVResponse{
		Key:     key,
		Value:   req.Value,
		Message: "Key updated successfully",
	})
}

func (s *Server) handleDeleteByPath(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/api/kv/")
	if key == "" {
		key = r.URL.Query().Get("key")
	}
	if key == "" {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "Key parameter required"})
		return
	}

	if err := s.db.Delete([]byte(key)); err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("Failed to delete key: %v", err)})
		return
	}

	respondJSON(w, http.StatusOK, KVResponse{
		Key:     key,
		Message: "Key deleted successfully",
	})
}

// ---------------------------------------------------------
// Telemetry & Admin Handlers
// ---------------------------------------------------------

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := s.limiter.GetMetrics()
	respondJSON(w, http.StatusOK, metrics)
}

func (s *Server) handleConfigRateLimiter(w http.ResponseWriter, r *http.Request) {
	var cfg ConfigRequest
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid JSON payload"})
		return
	}

	strat := middleware.StrategyType(cfg.Strategy)
	switch strat {
	case middleware.StrategySlidingWindowCounter, middleware.StrategyLeakyBucket, middleware.StrategyTokenBucket:
	default:
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid strategy type. Valid values: sliding_window, leaky_bucket, token_bucket"})
		return
	}

	if cfg.Limit <= 0 {
		cfg.Limit = 10
	}
	if cfg.WindowSeconds <= 0 {
		cfg.WindowSeconds = 60
	}

	window := time.Duration(cfg.WindowSeconds) * time.Second
	s.limiter.SetStrategy(strat, cfg.Limit, window)

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"message":        "Rate limiter strategy updated successfully",
		"active_strategy": string(strat),
		"limit":          cfg.Limit,
		"window_seconds": cfg.WindowSeconds,
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, http.StatusOK, map[string]string{
		"status": "healthy",
		"engine": "lsm-tree",
	})
}

func respondJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}
