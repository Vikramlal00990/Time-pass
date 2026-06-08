package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"runtime"
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

type FetchRequest struct {
	Site    string            `json:"site"`
	Key     string            `json:"key"`
	URL     string            `json:"url"`
	Method  string            `json:"method,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    string            `json:"body,omitempty"`
	Proxy   string            `json:"proxy,omitempty"`
}

type FetchResponse struct {
	Success bool              `json:"success"`
	Status  int               `json:"status,omitempty"`
	Body    string            `json:"body,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Cookie  string            `json:"cookie,omitempty"`
	Error   string            `json:"error,omitempty"`
}

type Response struct {
	Success bool   `json:"success"`
	Result  string `json:"result,omitempty"`
	Status  int    `json:"status,omitempty"`
	Error   string `json:"error,omitempty"`
}

type Job struct {
	Req        Request
	ResultChan chan Response
}

var (
	MaxWorkers = runtime.NumCPU() * 32
	jobQueue   = make(chan Job, 20000)
)

// Reusable chrome client pool
var clientPool = sync.Pool{
	New: func() interface{} {
		c, _ := solver.NewChromeTransport("")
		return c
	},
}

func startWorkerPool() {
	for i := 0; i < MaxWorkers; i++ {
		go func() {
			for job := range jobQueue {
				job.ResultChan <- processSolve(job.Req)
			}
		}()
	}
	log.Printf("✅ %d workers running", MaxWorkers)
}

func processSolve(req Request) Response {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	opts := []solver.Option{
		solver.WithDDJSKey(req.Key),
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

	if req.Verify {
		status, _, err := client.Verify(ctx, result.Cookie)
		if err != nil {
			return Response{Success: false, Error: err.Error()}
		}
		return Response{Success: true, Result: result.Cookie, Status: status}
	}

	return Response{Success: true, Result: result.Cookie}
}

func extractDatadomeCookie(cookie string) string {
	const prefix = "datadome="
	start := 0
	for i := 0; i <= len(cookie)-len(prefix); i++ {
		if cookie[i:i+len(prefix)] == prefix {
			start = i + len(prefix)
			break
		}
	}
	token := cookie[start:]
	for i, c := range token {
		if c == ';' || c == ' ' {
			return token[:i]
		}
	}
	return token
}

func processFetch(req FetchRequest) FetchResponse {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	chromeClient, err := solver.NewChromeTransport(req.Proxy)
	if err != nil {
		return FetchResponse{Success: false, Error: err.Error()}
	}

	opts := []solver.Option{
		solver.WithDDJSKey(req.Key),
		solver.WithHTTPClient(chromeClient),
	}
	if req.Proxy != "" {
		opts = append(opts, solver.WithProxy(req.Proxy))
	}

	client, err := solver.New(req.Site, opts...)
	if err != nil {
		return FetchResponse{Success: false, Error: err.Error()}
	}

	result, err := client.SolveWith(ctx, solver.SolveOptions{BPC: 1})
	if err != nil {
		return FetchResponse{Success: false, Error: err.Error()}
	}

	method := req.Method
	if method == "" {
		method = "GET"
	}
	targetURL := req.URL
	if targetURL == "" {
		targetURL = req.Site
	}

	var bodyReader io.Reader
	if req.Body != "" {
		bodyReader = bytes.NewBufferString(req.Body)
	}

	httpReq, err := http.NewRequestWithContext(ctx, method, targetURL, bodyReader)
	if err != nil {
		return FetchResponse{Success: false, Error: err.Error()}
	}

	httpReq.Header.Set("cookie", result.Cookie)
	httpReq.Header.Set("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36")
	httpReq.Header.Set("accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8")
	httpReq.Header.Set("accept-language", "en-US,en;q=0.9")
	httpReq.Header.Set("sec-fetch-dest", "document")
	httpReq.Header.Set("sec-fetch-mode", "navigate")
	httpReq.Header.Set("sec-fetch-site", "none")

	for k, v := range req.Headers {
		httpReq.Header.Set(k, v)
	}

	resp, err := chromeClient.Do(httpReq)
	if err != nil {
		return FetchResponse{Success: false, Error: err.Error()}
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return FetchResponse{Success: false, Error: err.Error()}
	}

	respHeaders := make(map[string]string)
	for k, v := range resp.Header {
		if len(v) > 0 {
			respHeaders[k] = v[0]
		}
	}

	return FetchResponse{
		Success: true,
		Status:  resp.StatusCode,
		Body:    string(respBody),
		Headers: respHeaders,
		Cookie:  extractDatadomeCookie(result.Cookie),
	}
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

func fetchHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(FetchResponse{Success: false, Error: "method not allowed"})
		return
	}
	var req FetchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		json.NewEncoder(w).Encode(FetchResponse{Success: false, Error: "invalid json"})
		return
	}
	if req.Site == "" || req.Key == "" {
		json.NewEncoder(w).Encode(FetchResponse{Success: false, Error: "site and key required"})
		return
	}
	result := processFetch(req)
	json.NewEncoder(w).Encode(result)
}

func encryptHandler(w http.ResponseWriter, r *http.Request) {
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
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	opts := []solver.Option{solver.WithDDJSKey(req.Key)}
	if req.Proxy != "" {
		opts = append(opts, solver.WithProxy(req.Proxy))
	}
	client, err := solver.New(req.Site, opts...)
	if err != nil {
		json.NewEncoder(w).Encode(Response{Success: false, Error: err.Error()})
		return
	}
	signals := client.BuildPayload(nil, 1)
	jspl, err := client.EncryptJSPL(signals)
	if err != nil {
		json.NewEncoder(w).Encode(Response{Success: false, Error: err.Error()})
		return
	}
	_ = ctx
	json.NewEncoder(w).Encode(Response{Success: true, Result: jspl})
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"workers":    MaxWorkers,
		"queue_size": len(jobQueue),
		"queue_cap":  cap(jobQueue),
	})
}

func warmup() {
	// Pre-fetch tags endpoint
	http.Get("https://api-js.datadome.co/js/")
	http.Get("https://api-js.datadome.co/js/")
	time.Sleep(500 * time.Millisecond)
	for i := 0; i < 10; i++ {
		go processSolve(Request{
			Site: "https://seatgeek.com",
			Key:  "60D428DD4BC75DF55D205B3DBE4AFF",
		})
	}
	log.Println("🔥 Warmup done")
}

func health(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("OK"))
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	go startWorkerPool()
	go warmup()

	mux := http.NewServeMux()
	mux.HandleFunc("/solve", solveHandler)
	mux.HandleFunc("/fetch", fetchHandler)
	mux.HandleFunc("/encrypt", encryptHandler)
	mux.HandleFunc("/status", statusHandler)
	mux.HandleFunc("/health", health)

	srv := &http.Server{
		Addr:         "0.0.0.0:" + port,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 40 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	log.Printf("🚀 Server :%s | %d workers | CPUs: %d", port, MaxWorkers, runtime.NumCPU())
	log.Fatal(srv.ListenAndServe())
}
