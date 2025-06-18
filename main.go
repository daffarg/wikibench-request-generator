package main

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"math/rand"
	"net/http"

	"github.com/joho/godotenv"
	"go.uber.org/zap"
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

func startSimulation(durationMinutes, bufferSize, workerCount, reductionPermil, pollIntervalMinutes int, stop <-chan struct{}) {
	traceDirPath := os.Getenv("TRACE_DIR_PATH")
	targetURL := os.Getenv("TARGET_URL")

	if traceDirPath == "" || targetURL == "" {
		logger.Fatal("TRACE_DIR_PATH and TARGET_URL must be set")
		return
	}

	startTime := time.Now()
	var firstTimestamp float64 = -1
	processedFiles := make(map[string]bool)

	requests := make(chan Request, bufferSize)
	var wg sync.WaitGroup

	logger.Info("Request simulation started",
		zap.Int64("timestamp", time.Now().Unix()),
		zap.Int("duration_minutes", durationMinutes),
		zap.Int("poll_interval_minutes", pollIntervalMinutes),
		zap.String("trace_directory", traceDirPath),
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

	for {
		select {
		case <-stop:
			logger.Info("Simulation stopped externally, shutting down polling loop.")
			goto end_simulation
		default:
		}

		allFiles, err := os.ReadDir(traceDirPath)
		if err != nil {
			logger.Error("Failed to read trace directory, will retry later.",
				zap.String("directory", traceDirPath),
				zap.Error(err),
			)
			time.Sleep(time.Duration(pollIntervalMinutes) * time.Minute)
			continue
		}

		var newFiles []os.DirEntry
		for _, fileEntry := range allFiles {
			if !fileEntry.IsDir() && strings.HasSuffix(fileEntry.Name(), ".trace") && !processedFiles[fileEntry.Name()] {
				newFiles = append(newFiles, fileEntry)
			}
		}

		if len(newFiles) > 0 {
			logger.Info("New trace files detected", zap.Int("count", len(newFiles)))

			sort.Slice(newFiles, func(i, j int) bool {
				tsI, _ := strconv.ParseInt(strings.TrimSuffix(newFiles[i].Name(), ".trace"), 10, 64)
				tsJ, _ := strconv.ParseInt(strings.TrimSuffix(newFiles[j].Name(), ".trace"), 10, 64)
				return tsI < tsJ
			})

			for _, fileEntry := range newFiles {
				filePath := fmt.Sprintf("%s/%s", traceDirPath, fileEntry.Name())
				logger.Info("Processing new trace file", zap.String("file", filePath))

				file, err := os.Open(filePath)
				if err != nil {
					logger.Error("Failed to open trace file", zap.String("file", filePath), zap.Error(err))
					continue
				}

				scanner := bufio.NewScanner(file)
				for scanner.Scan() {
					select {
					case <-stop:
						logger.Info("Simulation stopped externally during file processing.")
						file.Close()
						goto end_simulation
					default:
					}

					line := scanner.Text()
					parts := strings.Split(line, " ")
					if len(parts) < 3 {
						continue
					}

					reqTimestamp := parts[0]

					if len(parts[1]) <= 12 {
						logger.Info("Ignoring faulty URL", zap.String("url", parts[1]))
						continue
					}

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

					if strings.Contains(path, "?search=") || strings.Contains(path, "&search=") || strings.HasPrefix(path, "wiki/Special:Search") ||
						strings.HasPrefix(path, "w/query.php") ||
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
						logger.Info("Simulation duration limit reached.")
						file.Close()
						goto end_simulation
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
				file.Close()
				processedFiles[fileEntry.Name()] = true
				logger.Info("Finished processing file", zap.String("file", filePath))
			}
		} else {
			logger.Info("No new trace files found, waiting for next poll.")
		}

		logger.Info("Waiting for next polling cycle", zap.Int("minutes", pollIntervalMinutes))
		time.Sleep(time.Duration(pollIntervalMinutes) * time.Minute)
	}

end_simulation:
	close(requests)
	wg.Wait()
	logger.Info("Request simulation completed", zap.Int64("timestamp", time.Now().Unix()))
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
	durationMinutes := -1
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

	pollIntervalStr := r.URL.Query().Get("poll_interval")
	pollIntervalMinutes := 15
	if pollIntervalStr != "" {
		if val, err := strconv.Atoi(pollIntervalStr); err == nil && val > 0 {
			pollIntervalMinutes = val
		}
	}

	stopChan = make(chan struct{})
	simulationWg.Add(1)
	go func() {
		defer simulationWg.Done()
		startSimulation(durationMinutes, bufferSize, workerCount, reductionPermil, pollIntervalMinutes, stopChan)
	}()

	responseMsg := fmt.Sprintf("Real-time simulation started with duration %d minutes, buffer %d, workers %d, reduction %d‰, and polling interval %d minutes.",
		durationMinutes, bufferSize, workerCount, reductionPermil, pollIntervalMinutes)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(responseMsg))
}

func stopHandler(w http.ResponseWriter, r *http.Request) {
	simulationMu.Lock()

	if !simulationRun {
		simulationMu.Unlock()
		logger.Warn("Stop requested but no simulation is running")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("No simulation is running"))
		return
	}

	close(stopChan)
	simulationMu.Unlock()

	simulationWg.Wait()

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Simulation stopped"))
}

func init() {
	godotenv.Load()
}

func main() {
	var err error
	logger, err = zap.NewProduction()
	if err != nil {
		fmt.Printf("can't initialize zap logger: %v", err)
		os.Exit(1)
	}
	defer logger.Sync()

	http.HandleFunc("/start", startHandler)
	http.HandleFunc("/stop", stopHandler)
	logger.Info("Server started on port 8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		logger.Fatal("Failed to start server", zap.Error(err))
	}
}
