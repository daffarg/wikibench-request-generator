package main

import (
	"bufio"
	"container/heap"
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

type Request struct {
	ID        int
	Timestamp float64
	Index     int
}

type RequestHeap []Request

func (h RequestHeap) Len() int            { return len(h) }
func (h RequestHeap) Less(i, j int) bool  { return h[i].Timestamp < h[j].Timestamp }
func (h RequestHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *RequestHeap) Push(x interface{}) { *h = append(*h, x.(Request)) }
func (h *RequestHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

func sendRequest(url string, delay time.Duration, wg *sync.WaitGroup, id int) {
	defer wg.Done()
	time.Sleep(delay)
	resp, err := http.Get(url)
	if err != nil {
		logger.Error("Error while sending request",
			zap.String("error", err.Error()),
			zap.Int("event_id", id),
		)
		return
	}
	defer resp.Body.Close()

	logger.Info("Request sent",
		zap.Int("status_code", resp.StatusCode),
		zap.Int("event_id", id))
}

func startSimulation() {
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

	scanner := bufio.NewScanner(file)
	var requestHeap RequestHeap
	heap.Init(&requestHeap)

	firstTimestamp := -1.0
	startTime := time.Now()
	var wg sync.WaitGroup

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, " ")
		if len(parts) < 3 {
			continue
		}

		id, err1 := strconv.Atoi(parts[0])
		ts, err2 := strconv.ParseFloat(parts[1], 64)
		if err1 != nil || err2 != nil {
			continue
		}

		if firstTimestamp == -1.0 {
			firstTimestamp = ts
		}

		heap.Push(&requestHeap, Request{ID: id, Timestamp: ts})

		for requestHeap.Len() > 0 {
			req := heap.Pop(&requestHeap).(Request)
			delay := time.Duration((req.Timestamp - firstTimestamp) * float64(time.Second))
			deltaTime := time.Since(startTime)

			if delay > deltaTime {
				time.Sleep(delay - deltaTime)
			}

			wg.Add(1)
			go sendRequest(targetURL, 0, &wg, req.ID)
		}
	}

	wg.Wait()
	logger.Info("Request simulation completed")
}

func startHandler(w http.ResponseWriter, r *http.Request) {
	go startSimulation()
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Real-time simulation started"))
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
