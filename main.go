package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/joho/godotenv"
	"go.uber.org/zap"
)

var (
	logger          *zap.Logger
	stopChan        chan struct{}
	simulationWg    sync.WaitGroup
	simulationMu    sync.Mutex
	simulationRun   bool
	simulationState string
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
	FileName  string
	Timestamp float64
}

type TrafficPhase struct {
	PhaseName          string `json:"phase_name"`
	DurationMinutesMin int    `json:"duration_minutes_min"`
	DurationMinutesMax int    `json:"duration_minutes_max"`
	ReductionPermilMin int    `json:"reduction_permil_min"`
	ReductionPermilMax int    `json:"reduction_permil_max"`
}

func sendRequest(url string, timestamp float64, fileName string) {
	resp, err := client.Get(url)
	if err != nil {
		logger.Error("Error while sending request", zap.String("error", err.Error()), zap.Float64("timestamp", timestamp), zap.String("file_name", fileName))
		return
	}
	defer resp.Body.Close()
	logger.Info("Request sent", zap.Int("status_code", resp.StatusCode), zap.Float64("timestamp", timestamp), zap.String("file_name", fileName))
}

func sampleRequest(reduction int) bool {
	if reduction <= 0 {
		return true
	}
	if reduction >= 1000 {
		return false
	}
	return rand.Intn(1000) >= reduction
}

func processLine(line, fileName string, startTime time.Time, firstTimestamp float64, durationLimit float64, reduction int, wg *sync.WaitGroup, requests chan<- Request, stop <-chan struct{}) (float64, bool) {
	parts := strings.Split(line, " ")
	if len(parts) < 3 {
		return firstTimestamp, false
	}
	reqTimestamp := parts[0]

	if len(parts[1]) <= 12 {
		logger.Warn("Ignoring faulty or short URL entry", zap.String("url_part", parts[1]))
		return firstTimestamp, false
	}

	url := parts[1][7:]
	if !strings.HasPrefix(url, "en.wikipedia.org") {
		return firstTimestamp, false
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
		return firstTimestamp, false
	}

	ts, err := strconv.ParseFloat(reqTimestamp, 64)
	if err != nil {
		return firstTimestamp, false
	}

	if firstTimestamp < 0 {
		firstTimestamp = ts
	}

	delay := time.Duration((ts - firstTimestamp) * float64(time.Second))
	if durationLimit >= 0 && (ts-firstTimestamp) > durationLimit {
		logger.Info("Simulation duration limit reached.")
		return firstTimestamp, true
	}

	timeElapsed := time.Since(startTime)
	if delay > timeElapsed {
		sleepDuration := delay - timeElapsed
		sleepTimer := time.NewTimer(sleepDuration)
		select {
		case <-stop:
			logger.Info("Stop signal received during delay sleep.")
			sleepTimer.Stop()
			return firstTimestamp, true
		case <-sleepTimer.C:
		}
	}

	if sampleRequest(reduction) {
		wg.Add(1)
		requests <- Request{Timestamp: ts, FileName: fileName}
	}
	return firstTimestamp, false
}

func startSimulation(durationMinutes, bufferSize, workerCount int, pollIntervalMinutes int, singleTraceFile string, stop <-chan struct{}) {
	traceDirPath := os.Getenv("TRACE_DIR_PATH")
	targetURL := os.Getenv("TARGET_URL")

	if traceDirPath == "" || targetURL == "" {
		logger.Fatal("TRACE_DIR_PATH and TARGET_URL must be set")
		return
	}

	var trafficSchedule []TrafficPhase
	scheduleFilePath := filepath.Join(traceDirPath, "schedule.json")
	scheduleFileContent, err := os.ReadFile(scheduleFilePath)
	if err != nil {
		logger.Fatal("schedule.json not found or failed to read.", zap.Error(err), zap.String("path", scheduleFilePath))
		return
	}
	if err := json.Unmarshal(scheduleFileContent, &trafficSchedule); err != nil {
		logger.Fatal("Failed to parse schedule.json", zap.Error(err))
		return
	}
	logger.Info("Successfully loaded traffic schedule", zap.Int("phases", len(trafficSchedule)))

	startTime := time.Now()
	var firstTimestamp float64 = -1
	requests := make(chan Request, bufferSize)
	var wg sync.WaitGroup

	simulationMu.Lock()
	simulationRun = true
	simulationState = "STARTING"
	simulationMu.Unlock()

	defer func() {
		close(requests)
		wg.Wait()
		simulationMu.Lock()
		simulationRun = false
		simulationState = "STOPPED"
		simulationMu.Unlock()
		logger.Info("Request simulation completed", zap.Int64("timestamp", time.Now().Unix()))
	}()

	for i := 0; i < workerCount; i++ {
		go func() {
			for req := range requests {
				sendRequest(targetURL, req.Timestamp, req.FileName)
				wg.Done()
			}
		}()
	}

	durationLimit := -1.0
	if durationMinutes > 0 {
		durationLimit = float64(durationMinutes * 60)
	}

	if singleTraceFile != "" {
		simulationMu.Lock()
		simulationState = "RUNNING"
		simulationMu.Unlock()

		fullPath := filepath.Join(traceDirPath, singleTraceFile)
		file, err := os.Open(fullPath)
		if err != nil {
			logger.Error("Failed to open trace file for single-file mode", zap.Error(err), zap.String("path", fullPath))
			return
		}
		defer file.Close()

		for _, phase := range trafficSchedule {
			select {
			case <-stop:
				logger.Info("Simulation stopped externally.")
				return
			default:
			}

			logger.Info("Starting new traffic phase for single file", zap.String("phase", phase.PhaseName))

			_, err := file.Seek(0, io.SeekStart) // "Rewind" file ke awal untuk setiap fase
			if err != nil {
				logger.Error("Failed to rewind trace file", zap.Error(err))
				return
			}

			randDuration := time.Duration(phase.DurationMinutesMin) * time.Minute
			if phase.DurationMinutesMax > phase.DurationMinutesMin {
				randDuration += time.Duration(rand.Intn(phase.DurationMinutesMax-phase.DurationMinutesMin+1)) * time.Minute
			}
			phaseEndTime := time.Now().Add(randDuration)

			scanner := bufio.NewScanner(file)
			for scanner.Scan() && time.Now().Before(phaseEndTime) {
				select {
				case <-stop:
					return
				default:
				}

				randReduction := phase.ReductionPermilMin
				if phase.ReductionPermilMax > phase.ReductionPermilMin {
					randReduction += rand.Intn(phase.ReductionPermilMax - phase.ReductionPermilMin + 1)
				}

				var shouldStop bool
				firstTimestamp, shouldStop = processLine(scanner.Text(), singleTraceFile, startTime, firstTimestamp, durationLimit, randReduction, &wg, requests, stop)
				if shouldStop {
					return
				}
			}
			logger.Info("Finished phase", zap.String("phase", phase.PhaseName))
		}
		logger.Info("All phases completed for single file.")

	} else {
		currentPhaseIndex := 0
		processedFiles := make(map[string]bool)
		for {
			select {
			case <-stop:
				logger.Info("Simulation stopped externally.")
				return
			default:
			}

			allFiles, _ := os.ReadDir(traceDirPath)
			var newFiles []os.DirEntry
			for _, fileEntry := range allFiles {
				if !fileEntry.IsDir() && strings.HasSuffix(fileEntry.Name(), ".trace") && !processedFiles[fileEntry.Name()] {
					newFiles = append(newFiles, fileEntry)
				}
			}

			if len(newFiles) == 0 {
				simulationMu.Lock()
				simulationState = "IDLE"
				simulationMu.Unlock()
				logger.Info("No new trace files found, waiting for next poll.")
				time.Sleep(time.Duration(pollIntervalMinutes) * time.Minute)
				continue
			}

			simulationMu.Lock()
			simulationState = "RUNNING"
			simulationMu.Unlock()

			sort.Slice(newFiles, func(i, j int) bool {
				tsI, _ := strconv.ParseInt(strings.TrimSuffix(newFiles[i].Name(), ".trace"), 10, 64)
				tsJ, _ := strconv.ParseInt(strings.TrimSuffix(newFiles[j].Name(), ".trace"), 10, 64)
				return tsI < tsJ
			})

			logger.Info("New trace files detected, processing batch.", zap.Int("count", len(newFiles)))
			for _, fileEntry := range newFiles {
				currentPhase := trafficSchedule[currentPhaseIndex]
				filePath := filepath.Join(traceDirPath, fileEntry.Name())
				logger.Info("Applying phase to new file", zap.String("phase", currentPhase.PhaseName), zap.String("file", filePath))

				file, err := os.Open(filePath)
				if err != nil {
					logger.Error("Failed to open trace file", zap.Error(err))
					continue
				}

				scanner := bufio.NewScanner(file)
				for scanner.Scan() {
					select {
					case <-stop:
						file.Close()
						return
					default:
					}
					firstTimestamp, _ = processLine(scanner.Text(), fileEntry.Name(), startTime, firstTimestamp, durationLimit, currentPhase.ReductionPermilMin, &wg, requests, stop)
				}
				file.Close()

				processedFiles[fileEntry.Name()] = true
				logger.Info("Finished processing file", zap.String("file", filePath))
				currentPhaseIndex = (currentPhaseIndex + 1) % len(trafficSchedule)
			}
		}
	}
}

func startHandler(w http.ResponseWriter, r *http.Request) {
	simulationMu.Lock()
	defer simulationMu.Unlock()
	if simulationRun {
		w.WriteHeader(http.StatusConflict)
		w.Write([]byte("Simulation is already running"))
		return
	}

	traceFile := r.URL.Query().Get("trace_file")
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
		startSimulation(durationMinutes, bufferSize, workerCount, pollIntervalMinutes, traceFile, stopChan)
	}()

	var responseMsg string
	if traceFile != "" {
		responseMsg = fmt.Sprintf("Single file simulation started for file: %s", traceFile)
	} else {
		responseMsg = fmt.Sprintf("Polling simulation started with polling interval %d minutes.", pollIntervalMinutes)
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(responseMsg))
}

func stopHandler(w http.ResponseWriter, r *http.Request) {
	simulationMu.Lock()
	if !simulationRun {
		simulationMu.Unlock()
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("No simulation is running"))
		return
	}
	simulationState = "STOPPING"
	close(stopChan)
	simulationMu.Unlock()
	simulationWg.Wait()
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Simulation stopped"))
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	simulationMu.Lock()
	defer simulationMu.Unlock()
	state := simulationState
	if !simulationRun && state == "" {
		state = "STOPPED"
	}
	response := fmt.Sprintf(`{"status": "%s"}`, state)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(response))
}

func init() {
	rand.Seed(time.Now().UnixNano())
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
	http.HandleFunc("/status", statusHandler)
	logger.Info("Server started on port 8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		logger.Fatal("Failed to start server", zap.Error(err))
	}
}
