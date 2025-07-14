package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
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

func runSingleFileSimulation(fileName string, startTime time.Time, durationLimit float64, trafficSchedule []TrafficPhase, wg *sync.WaitGroup, requests chan<- Request, stop <-chan struct{}, infoPhase string) {
	traceDirPath := os.Getenv("TRACE_DIR_PATH")
	fullPath := filepath.Join(traceDirPath, fileName)
	file, err := os.Open(fullPath)
	if err != nil {
		logger.Error("Failed to open trace file", zap.Error(err), zap.String("path", fullPath))
		return
	}
	defer file.Close()

	simulationMu.Lock()
	simulationState = "RUNNING"
	simulationMu.Unlock()

	currentPhaseIndex := -1
	var phaseEndTime time.Time
	var currentPhase TrafficPhase
	var firstTimestamp float64 = -1

	// For info_phase tracking
	var phaseStartMs int64
	var phaseReduction int
	var phaseDuration time.Duration

	logger.Info("Starting single file simulation",
		zap.String("file", fileName),
		zap.Int("total_phases", len(trafficSchedule)),
	)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		select {
		case <-stop:
			logger.Info("Simulation stopped externally.")
			return
		default:
		}

		// detect phase boundary
		if time.Now().After(phaseEndTime) {
			// send info for previous phase
			if infoPhase == "true" && currentPhaseIndex >= 0 {
				// wait until requests channel is drained
				for len(requests) > 0 {
					time.Sleep(5 * time.Second)
					logger.Info("Waiting for requests to drain before sending phase info",
						zap.Int("remaining_requests", len(requests)),
						zap.String("phase", currentPhase.PhaseName),
					)
				}
				endMs := time.Now().Unix()

				payload := map[string]interface{}{
					"start_timestamp": phaseStartMs,
					"end_timestamp":   endMs,
					"phase_name":      currentPhase.PhaseName,
					"reduction":       phaseReduction,
					"duration":        phaseDuration.Minutes(),
				}

				// send POST to /single/phase
				host := os.Getenv("EVAL_HOST")
				bts, _ := json.Marshal(payload)
				req, _ := http.NewRequest("POST", fmt.Sprintf("%s/single/phase", host), bytes.NewReader(bts))
				req.Header.Set("Content-Type", "application/json")
				cli := &http.Client{Timeout: 10 * time.Second}
				resp, err := cli.Do(req)
				if err != nil {
					logger.Error("Failed to send phase info",
						zap.String("host", host),
						zap.String("phase", currentPhase.PhaseName),
						zap.Error(err),
					)
				} else {
					resp.Body.Close()
					logger.Info("Phase info sent", zap.String("phase", currentPhase.PhaseName))
				}
				time.Sleep(5 * time.Second)
			}

			// start next phase
			currentPhaseIndex++
			if currentPhaseIndex >= len(trafficSchedule) {
				logger.Info("All schedule phases completed for single file.")
				break
			}
			currentPhase = trafficSchedule[currentPhaseIndex]
			// randomize reduction & duration
			randDuration := time.Duration(currentPhase.DurationMinutesMin) * time.Minute
			if currentPhase.DurationMinutesMax > currentPhase.DurationMinutesMin {
				randDuration += time.Duration(rand.Intn(currentPhase.DurationMinutesMax-currentPhase.DurationMinutesMin+1)) * time.Minute
			}
			phaseDuration = randDuration
			phaseEndTime = time.Now().Add(phaseDuration)
			phaseStartMs = time.Now().Unix()
			randRed := currentPhase.ReductionPermilMin
			if currentPhase.ReductionPermilMax > randRed {
				randRed += rand.Intn(currentPhase.ReductionPermilMax - currentPhase.ReductionPermilMin + 1)
			}
			phaseReduction = randRed

			logger.Info("Entering new traffic phase", zap.String("phase", currentPhase.PhaseName), zap.Duration("duration", phaseDuration))
		}

		randReduction := phaseReduction
		firstTimestamp, _ = processLine(scanner.Text(), fileName, startTime, firstTimestamp, durationLimit, randReduction, wg, requests, stop)
		// continue to next line
	}
	logger.Info("Finished processing single file based on schedule.")
}

func runPollingSimulation(durationLimit float64, pollIntervalMinutes int, trafficSchedule []TrafficPhase, wg *sync.WaitGroup, requests chan<- Request, stop <-chan struct{}) {
	traceDirPath := os.Getenv("TRACE_DIR_PATH")
	// Global phase control
	currentPhaseIndex := 0
	// Randomize initial phase duration
	getRandDuration := func(p TrafficPhase) time.Duration {
		d := time.Duration(p.DurationMinutesMin) * time.Minute
		if p.DurationMinutesMax > p.DurationMinutesMin {
			d += time.Duration(rand.Intn(p.DurationMinutesMax-p.DurationMinutesMin+1)) * time.Minute
		}
		return d
	}
	phase := trafficSchedule[currentPhaseIndex]
	phaseEndTime := time.Now().Add(getRandDuration(phase))
	logger.Info("Starting polling simulation with global phase", zap.String("phase", phase.PhaseName), zap.Time("until", phaseEndTime))

	// Track per-file offsets (lines) and first timestamps
	fileOffsets := make(map[string]int)
	fileFirstTimestamps := make(map[string]float64)
	processedFiles := make(map[string]bool)

	for {
		select {
		case <-stop:
			logger.Info("Simulation stopped externally.")
			return
		default:
		}

		// Discover new or unfinished trace files
		allEntries, _ := os.ReadDir(traceDirPath)
		var toProcess []os.DirEntry
		for _, ent := range allEntries {
			if ent.IsDir() || !strings.HasSuffix(ent.Name(), ".trace") {
				continue
			}
			if !processedFiles[ent.Name()] {
				toProcess = append(toProcess, ent)
			}
		}
		if len(toProcess) == 0 {
			simulationMu.Lock()
			simulationState = "IDLE"
			simulationMu.Unlock()
			logger.Info("No trace files to process, sleeping until next poll.")
			time.Sleep(time.Duration(pollIntervalMinutes) * time.Minute)
			continue
		}

		simulationMu.Lock()
		simulationState = "RUNNING"
		simulationMu.Unlock()
		sort.Slice(toProcess, func(i, j int) bool {
			ti, _ := strconv.ParseInt(strings.TrimSuffix(toProcess[i].Name(), ".trace"), 10, 64)
			tj, _ := strconv.ParseInt(strings.TrimSuffix(toProcess[j].Name(), ".trace"), 10, 64)
			return ti < tj
		})

		for _, entry := range toProcess {
			select {
			case <-stop:
				return
			default:
			}
			fileName := entry.Name()
			filePath := filepath.Join(traceDirPath, fileName)

			logger.Info("Processing trace file under current phase",
				zap.String("file", fileName), zap.String("phase", trafficSchedule[currentPhaseIndex].PhaseName), zap.Time("until", phaseEndTime))

			f, err := os.Open(filePath)
			if err != nil {
				logger.Error("Failed to open trace file", zap.Error(err), zap.String("path", filePath))
				processedFiles[fileName] = true
				continue
			}
			reader := bufio.NewReader(f)
			scanner := bufio.NewScanner(reader)

			// Resume from last offset (skip lines)
			skip := fileOffsets[fileName]
			for i := 0; i < skip; i++ {
				if !scanner.Scan() {
					break
				}
			}
			processedLines := skip
			firstTs := fileFirstTimestamps[fileName]
			if firstTs == 0 {
				firstTs = -1
			}

			// Process lines until phase end or EOF
			endedByPhase := false
			for scanner.Scan() {
				if time.Now().After(phaseEndTime) {
					endedByPhase = true
					break
				}
				line := scanner.Text()
				firstTs, _ = processLine(line, fileName, time.Now(), firstTs, durationLimit,
					func() int {
						red := trafficSchedule[currentPhaseIndex].ReductionPermilMin
						if trafficSchedule[currentPhaseIndex].ReductionPermilMax > red {
							red += rand.Intn(trafficSchedule[currentPhaseIndex].ReductionPermilMax - red + 1)
						}
						return red
					}(), wg, requests, stop)
				processedLines++
			}
			// Save state
			fileOffsets[fileName] = processedLines
			fileFirstTimestamps[fileName] = firstTs

			err = scanner.Err()
			if err != nil {
				logger.Error("Error during file scan", zap.Error(err), zap.String("file", fileName))
			}
			f.Close()

			// Determine if file done
			if !endedByPhase && (err == nil && !scanner.Scan()) {
				processedFiles[fileName] = true
				delete(fileOffsets, fileName)
				delete(fileFirstTimestamps, fileName)
				logger.Info("Trace file fully processed", zap.String("file", fileName))
			} else {
				logger.Info("Phase ended before file completion", zap.String("file", fileName), zap.String("phase", trafficSchedule[currentPhaseIndex].PhaseName))
			}

			// If phase time expired, rotate phase
			if time.Now().After(phaseEndTime) {
				currentPhaseIndex = (currentPhaseIndex + 1) % len(trafficSchedule)
				phase = trafficSchedule[currentPhaseIndex]
				phaseEndTime = time.Now().Add(getRandDuration(phase))
				logger.Info("Rotated to next phase", zap.String("newPhase", phase.PhaseName), zap.Time("until", phaseEndTime))
			}
		}
	}
}

func startSimulation(
	durationMinutes, bufferSize, workerCount int,
	pollIntervalMinutes int, infoPhase, singleTraceFile string,
	stop <-chan struct{},
) {
	requests := make(chan Request, bufferSize)
	var wg sync.WaitGroup

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
				sendRequest(os.Getenv("TARGET_URL"), req.Timestamp, req.FileName)
				wg.Done()
			}
		}()
	}

	traceDirPath := os.Getenv("TRACE_DIR_PATH")
	if traceDirPath == "" {
		logger.Fatal("TRACE_DIR_PATH must be set")
		return
	}
	var trafficSchedule []TrafficPhase
	scheduleFilePath := filepath.Join(traceDirPath, "schedule.json")
	content, err := os.ReadFile(scheduleFilePath)
	if err != nil {
		logger.Fatal("schedule.json not found or failed to read.", zap.Error(err))
		return
	}
	if err := json.Unmarshal(content, &trafficSchedule); err != nil {
		logger.Fatal("Failed to parse schedule.json", zap.Error(err))
		return
	}
	logger.Info("Successfully loaded traffic schedule", zap.Int("phases", len(trafficSchedule)))

	simulationMu.Lock()
	simulationRun = true
	simulationState = "STARTING"
	simulationMu.Unlock()

	startTime := time.Now()
	durationLimit := -1.0
	if durationMinutes > 0 {
		durationLimit = float64(durationMinutes * 60)
	}

	if singleTraceFile != "" {
		runSingleFileSimulation(singleTraceFile, startTime, durationLimit, trafficSchedule, &wg, requests, stop, infoPhase)
	} else {
		runPollingSimulation(durationLimit, pollIntervalMinutes, trafficSchedule, &wg, requests, stop)
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
	infoPhase := r.URL.Query().Get("info_phase")
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
		startSimulation(durationMinutes, bufferSize, workerCount, pollIntervalMinutes, infoPhase, traceFile, stopChan)
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
