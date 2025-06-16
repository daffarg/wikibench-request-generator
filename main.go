package main

import (
	"bufio"
	"github.com/joho/godotenv"
	"go.uber.org/zap"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	logger        *zap.Logger
	stopChan      chan struct{}
	simulationWg  sync.WaitGroup
	simulationMu  sync.Mutex
	simulationRun bool
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

func sampleRequest(reduction int) bool {
	if reduction <= 0 {
		return true
	}
	n := rand.Intn(1000)
	return n >= reduction
}

func startSimulation(durationMinutes, bufferSize, workerCount, reductionPermil int, stop <-chan struct{}) {
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

	requests := make(chan Request, bufferSize)
	var wg sync.WaitGroup

	logger.Info("Request simulation started",
		zap.Int64("timestamp", time.Now().Unix()),
		zap.Int("duration_minutes", durationMinutes),
		zap.Int("buffer_size", bufferSize),
		zap.Int("worker_count", workerCount),
		zap.Int("reduction_permil", reductionPermil),
	)

	rand.Seed(time.Now().UnixNano())

	for i := 0; i < workerCount; i++ {
		go func() {
			for req := range requests {
				sendRequest(targetURL, req.Timestamp)
				wg.Done()
			}
		}()
	}

	durationLimit := float64(durationMinutes * 60)

	simulationMu.Lock()
	simulationRun = true
	simulationMu.Unlock()
	defer func() {
		simulationMu.Lock()
		simulationRun = false
		simulationMu.Unlock()
	}()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		select {
		case <-stop:
			logger.Info("Simulation stopped externally")
			goto end_loop
		default:
		}

		line := scanner.Text()
		parts := strings.Split(line, " ")
		if len(parts) < 3 {
			continue
		}

		reqTimestamp := parts[0]
		url := parts[1][7:]

		if !strings.HasPrefix(url, "en.wikipedia.org") {
			continue
		}

		path := ""
		index := strings.Index(url, "/")
		if index != -1 {
			path = url[index+1:]
		}

		path = strings.Replace(path, "%2F", "/", -1)
		path = strings.Replace(path, "%20", " ", -1)
		path = strings.Replace(path, "&amp;", "&", -1)
		path = strings.Replace(path, "%3A", ":", -1)

		if strings.Contains(path, "?search=") || strings.Contains(path, "&search=") || strings.HasPrefix(path, "wiki/Special:Search") {
			continue
		}
		if strings.HasPrefix(path, "w/query.php") ||
			strings.HasPrefix(path, "wiki/Talk:") ||
			strings.Contains(path, "User+talk") ||
			strings.Contains(path, "User_talk") ||
			strings.HasPrefix(path, "wiki/Special:AutoLogin") ||
			strings.HasPrefix(path, "Special:UserLogin") ||
			strings.Contains(path, "User:") ||
			strings.Contains(path, "Talk:") ||
			strings.Contains(path, "&diff=") ||
			strings.Contains(path, "&action=rollback") ||
			strings.Contains(path, "Special:Watchlist") ||
			strings.HasPrefix(path, "w/api.php") {
			continue
		}

		ts, err := strconv.ParseFloat(reqTimestamp, 64)
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

		if sampleRequest(reductionPermil) {
			wg.Add(1)
			requests <- Request{Timestamp: ts}
		}
	}

end_loop:

	close(requests)

	wg.Wait()

	logger.Info("Request simulation completed",
		zap.Int64("timestamp", time.Now().Unix()),
	)
}

func startHandler(w http.ResponseWriter, r *http.Request) {
	simulationMu.Lock()
	defer simulationMu.Unlock()

	if simulationRun {
		w.WriteHeader(http.StatusConflict)
		w.Write([]byte("Simulation is already running"))
		return
	}

	durationStr := r.URL.Query().Get("duration")
	durationMinutes := 5
	if durationStr != "" {
		if val, err := strconv.Atoi(durationStr); err == nil {
			durationMinutes = val
		}
	}

	bufferStr := r.URL.Query().Get("buffer")
	bufferSize := 1000
	if bufferStr != "" {
		if val, err := strconv.Atoi(bufferStr); err == nil {
			bufferSize = val
		}
	}

	workerStr := r.URL.Query().Get("workers")
	workerCount := 100
	if workerStr != "" {
		if val, err := strconv.Atoi(workerStr); err == nil {
			workerCount = val
		}
	}

	reductionStr := r.URL.Query().Get("reduction")
	reductionPermil := 0
	if reductionStr != "" {
		if val, err := strconv.Atoi(reductionStr); err == nil {
			reductionPermil = val
		}
	}

	stopChan = make(chan struct{})
	simulationWg.Add(1)
	go func() {
		defer simulationWg.Done()
		startSimulation(durationMinutes, bufferSize, workerCount, reductionPermil, stopChan)
	}()

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Real-time simulation started for " +
		strconv.Itoa(durationMinutes) + " minutes with buffer size " +
		strconv.Itoa(bufferSize) + ", worker count " +
		strconv.Itoa(workerCount) + ", and reduction permil " +
		strconv.Itoa(reductionPermil)))
}

func stopHandler(w http.ResponseWriter, r *http.Request) {
	simulationMu.Lock()
	defer simulationMu.Unlock()

	if !simulationRun {
		logger.Warn("Stop requested but no simulation is running")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("No simulation is running"))
		return
	}

	close(stopChan)
	simulationWg.Wait()

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Simulation stopped"))
}

func init() {
	godotenv.Load()
}

func main() {
	logger, _ = zap.NewProduction()
	defer logger.Sync()

	http.HandleFunc("/start", startHandler)
	http.HandleFunc("/stop", stopHandler)
	logger.Info("Server started on port 8080")
	http.ListenAndServe(":8080", nil)
}
