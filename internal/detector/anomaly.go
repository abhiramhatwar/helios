package detector

import (
	"math"
	"sync"
	"time"
)

// sourceWindow tracks per-source event counts in rolling time buckets.
type sourceWindow struct {
	mu      sync.Mutex
	buckets []float64
	head    int
	current float64
}

func newSourceWindow(size int) *sourceWindow {
	return &sourceWindow{buckets: make([]float64, size)}
}

// flush commits the current bucket and advances the window.
func (w *sourceWindow) flush() {
	w.mu.Lock()
	w.buckets[w.head] = w.current
	w.head = (w.head + 1) % len(w.buckets)
	w.current = 0
	w.mu.Unlock()
}

// record increments the counter and returns the z-score of the current bucket.
func (w *sourceWindow) record() float64 {
	w.mu.Lock()
	w.current++
	cur := w.current
	n := float64(len(w.buckets))
	sum, sumSq := 0.0, 0.0
	for _, v := range w.buckets {
		sum += v
		sumSq += v * v
	}
	w.mu.Unlock()

	mean := sum / n
	variance := (sumSq / n) - (mean * mean)
	if variance <= 0 {
		return 0
	}
	return (cur - mean) / math.Sqrt(variance)
}

// Detector detects rate spikes per source using a sliding window z-score.
type Detector struct {
	mu         sync.RWMutex
	sources    map[string]*sourceWindow
	windowSize int
	bucketDur  time.Duration
	threshold  float64
	stopCh     chan struct{}
}

func New(windowSize int, bucketDur time.Duration, threshold float64) *Detector {
	d := &Detector{
		sources:    make(map[string]*sourceWindow),
		windowSize: windowSize,
		bucketDur:  bucketDur,
		threshold:  threshold,
		stopCh:     make(chan struct{}),
	}
	go d.flusher()
	return d
}

func (d *Detector) flusher() {
	ticker := time.NewTicker(d.bucketDur)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			d.mu.RLock()
			for _, w := range d.sources {
				w.flush()
			}
			d.mu.RUnlock()
		case <-d.stopCh:
			return
		}
	}
}

func (d *Detector) getOrCreate(source string) *sourceWindow {
	d.mu.RLock()
	w, ok := d.sources[source]
	d.mu.RUnlock()
	if ok {
		return w
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if w, ok = d.sources[source]; ok {
		return w
	}
	w = newSourceWindow(d.windowSize)
	d.sources[source] = w
	return w
}

// Record increments the event count for source and returns true if anomalous (z-score > threshold).
func (d *Detector) Record(source string) bool {
	return d.getOrCreate(source).record() > d.threshold
}

func (d *Detector) Stop() {
	close(d.stopCh)
}
