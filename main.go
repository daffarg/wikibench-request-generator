package main

import (
	"bufio"
	"github.com/joho/godotenv"
	"go.uber.org/zap"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	logger *zap.Logger
)

var client = &http.Client{
	Transport: &http.Transport{
		MaxIdleConns:        1000,
		MaxIdleConnsPerHost: 1000,
		MaxConnsPerHost:     1000,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,
		ForceAttemptHTTP2:   false,
	},
}

type Request struct {
	Timestamp float64
}

func sendRequest(url string, timestamp float64) {

	resp, err := client.Get(url)
	if err != nil {
		logger.Error("Error while sending request",
			zap.String("error", err.Error()),
			zap.Float64("timestamp", timestamp),
		)
		return
	}
	defer resp.Body.Close()

	logger.Info("Request sent",
		zap.Int("status_code", resp.StatusCode),
		zap.Float64("timestamp", timestamp),
	)
}

func startSimulation(durationMinutes int, bufferSize int, workerCount int) {
	traceFilePath := os.Getenv("TRACE_FILE_URL")
	targetURL := os.Getenv("TARGET_URL")
	if traceFilePath == "" || targetURL == "" {
		logger.Fatal("TRACE_FILE_URL and TARGET_URL must be set")
		return
	}

	file, err := os.Open(traceFilePath)
	if err != nil {
		logger.Error("Failed to open local trace file",
			zap.String("error", err.Error()))
		return
	}
	defer file.Close()

	startTime := time.Now()
	var firstTimestamp float64 = -1

	scanner := bufio.NewScanner(file)
	requests := make(chan Request, bufferSize) // buffered channel
	var wg sync.WaitGroup

	logger.Info("Request simulation started",
		zap.Int64("timestamp", time.Now().Unix()),
		zap.Int("duration_minutes", durationMinutes),
		zap.Int("buffer_size", bufferSize),
		zap.Int("worker_count", workerCount),
	)

	// Worker pool: limit concurrency to prevent OOM
	for i := 0; i < workerCount; i++ {
		go func() {
			for req := range requests {
				sendRequest(targetURL, req.Timestamp)
				wg.Done()
			}
		}()
	}

	durationLimit := float64(durationMinutes * 60) // convert to seconds

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, " ")
		if len(parts) < 3 {
			continue
		}

		ts, err := strconv.ParseFloat(parts[0], 64)
		if err != nil {
			continue
		}

		if firstTimestamp < 0 {
			firstTimestamp = ts
		}

		delay := time.Duration((ts - firstTimestamp) * float64(time.Second))
		if durationMinutes >= 0 && (ts-firstTimestamp) > durationLimit {
			break
		}

		timeElapsed := time.Since(startTime)
		if delay > timeElapsed {
			time.Sleep(delay - timeElapsed)
		}

		wg.Add(1)
		requests <- Request{Timestamp: ts}
	}

	wg.Wait()
	close(requests)
	logger.Info("Request simulation completed",
		zap.Int64("timestamp", time.Now().Unix()),
	)
}

func startHandler(w http.ResponseWriter, r *http.Request) {
	durationStr := r.URL.Query().Get("duration")
	durationMinutes := 5 // default
	if durationStr != "" {
		if val, err := strconv.Atoi(durationStr); err == nil {
			durationMinutes = val
		}
	}

	bufferStr := r.URL.Query().Get("buffer")
	bufferSize := 1000 // default
	if bufferStr != "" {
		if val, err := strconv.Atoi(bufferStr); err == nil {
			bufferSize = val
		}
	}
	workerStr := r.URL.Query().Get("workers")
	workerCount := 100 // default
	if workerStr != "" {
		if val, err := strconv.Atoi(workerStr); err == nil {
			workerCount = val
		}
	}
	go startSimulation(durationMinutes, bufferSize, workerCount)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Real-time simulation started for " + strconv.Itoa(durationMinutes) + " minutes with buffer size " + strconv.Itoa(bufferSize) + " and worker count " + strconv.Itoa(workerCount)))
}

func init() {
	godotenv.Load()
}

func main() {
	logger, _ = zap.NewProduction()
	defer logger.Sync()

	http.HandleFunc("/start", startHandler)
	logger.Info("Server started on port 8080")
	http.ListenAndServe(":8080", nil)
}
