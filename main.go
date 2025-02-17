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
	Index     int // for heap
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

func streamFromURL(url string) (*bufio.Scanner, func(), error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, nil, err
	}

	cleanup := func() {
		resp.Body.Close()
	}

	return bufio.NewScanner(resp.Body), cleanup, nil
}

func sendRequest(url string, delay time.Duration, wg *sync.WaitGroup) {
	defer wg.Done()
	time.Sleep(delay)
	resp, err := http.Get(url)
	if err != nil {
		logger.Error("Error while sending request",
			zap.String("error", err.Error()),
		)
		return
	}
	defer resp.Body.Close()

	logger.Info("Request sent",
		zap.Int("status_code", resp.StatusCode))
}

func startSimulation() {
	traceFileURL := os.Getenv("TRACE_FILE_URL")
	if traceFileURL == "" {
		logger.Error("TRACE_FILE_URL is not set in .env file")
		return
	}

	logger.Info("Streaming trace file from URL",
		zap.String("url", traceFileURL))

	scanner, cleanup, err := streamFromURL(traceFileURL)
	if err != nil {
		logger.Error("Error streaming trace file",
			zap.String("error", err.Error()),
		)
		return
	}
	defer cleanup()

	targetURL := os.Getenv("TARGET_URL")
	if targetURL == "" {
		logger.Error("TARGET_URL is not set in .env file")
		return
	}

	logger.Info("Starting request simulation",
		zap.String("target_url", targetURL))

	var requestHeap RequestHeap
	heap.Init(&requestHeap)

	startTime := time.Now()
	firstTimestamp := -1.0

	var wg sync.WaitGroup

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, " ")
		if len(parts) < 3 {
			continue
		}

		id, _ := strconv.Atoi(parts[0])
		timestamp, _ := strconv.ParseFloat(parts[1], 64)

		if firstTimestamp == -1.0 {
			firstTimestamp = timestamp
		}

		heap.Push(&requestHeap, Request{ID: id, Timestamp: timestamp})

		for requestHeap.Len() > 0 {
			req := heap.Pop(&requestHeap).(Request)
			delay := time.Duration((req.Timestamp - firstTimestamp) * float64(time.Second))
			deltaTime := time.Since(startTime)

			if delay > deltaTime {
				time.Sleep(delay - deltaTime)
			}

			wg.Add(1)
			go sendRequest(targetURL, 0, &wg)
		}
	}

	wg.Wait()
	logger.Info("Request simulation completed")
}

func startHandler(w http.ResponseWriter, r *http.Request) {
	go startSimulation()
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Simulation started"))
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
