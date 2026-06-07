package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/L0ed0/datadome-solver/pkg/solver"
)

type Request struct {
	Site     string `json:"site"`
	Key      string `json:"key,omitempty"`
	Proxy    string `json:"proxy,omitempty"`
	TwoPhase bool   `json:"two_phase,omitempty"`
	Delay    int    `json:"delay,omitempty"`
	Verify   bool   `json:"verify,omitempty"`
	Solve    bool   `json:"solve,omitempty"`
}

type Response struct {
	Success bool   `json:"success"`
	Result  string `json:"result,omitempty"`
	Error   string `json:"error,omitempty"`
}

type Job struct {
	Req        Request
	ResultChan chan Response
}

const MaxWorkers = 200

var jobQueue = make(chan Job, 5000)

var sharedHTTPClient = &http.Client{
	Timeout: 30 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        500,
		MaxIdleConnsPerHost: 200,
		IdleConnTimeout:     90 * time.Second,
	},
}

func startWorkerPool() {
	var wg sync.WaitGroup
	for i := 0; i < MaxWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for job := range jobQueue {
				job.ResultChan <- processSolve(job.Req)
			}
		}(i)
	}
	log.Printf("✅ %d workers running", MaxWorkers)
}

func processSolve(req Request) Response {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	opts := []solver.Option{
		solver.WithDDJSKey(req.Key),
		solver.WithHTTPClient(sharedHTTPClient),
	}
	if req.Proxy != "" {
		opts = append(opts, solver.WithProxy(req.Proxy))
	}

	client, err := solver.New(req.Site, opts...)
	if err != nil {
		return Response{Success: false, Error: err.Error()}
	}

	if req.TwoPhase {
		delay := time.Duration(req.Delay) * time.Second
		if delay == 0 {
			delay = 5 * time.Second
		}
		result, err := client.SolveTwoPhase(ctx, delay, "")
		if err != nil {
			return Response{Success: false, Error: err.Error()}
		}
		return Response{Success: true, Result: result.Cookie}
	}

	result, err := client.SolveWith(ctx, solver.SolveOptions{BPC: 1})
	if err != nil {
		return Response{Success: false, Error: err.Error()}
	}

	return Response{Success: true, Result: result.Cookie}
}

func solveHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "method not allowed"})
		return
	}
	var req Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		json.NewEncoder(w).Encode(Response{Success: false, Error: "invalid json"})
		return
	}
	if req.Site == "" || req.Key == "" {
		json.NewEncoder(w).Encode(Response{Success: false, Error: "site and key required"})
		return
	}
	resultChan := make(chan Response, 1)
	select {
	case jobQueue <- Job{Req: req, ResultChan: resultChan}:
	default:
		json.NewEncoder(w).Encode(Response{Success: false, Error: "queue full"})
		return
	}
	select {
	case result := <-resultChan:
		json.NewEncoder(w).Encode(result)
	case <-time.After(35 * time.Second):
		json.NewEncoder(w).Encode(Response{Success: false, Error: "timeout"})
	}
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"workers":    MaxWorkers,
		"queue_size": len(jobQueue),
		"queue_cap":  cap(jobQueue),
	})
}

func health(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("OK"))
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}
	go startWorkerPool()
	http.HandleFunc("/solve", solveHandler)
	http.HandleFunc("/status", statusHandler)
	http.HandleFunc("/health", health)
	log.Printf("🚀 Server :%s | %d workers | No subprocess", port, MaxWorkers)
	log.Fatal(http.ListenAndServe("0.0.0.0:"+port, nil))
}
