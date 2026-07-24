// Package demo implements the synthetic order and payment services that
// generate traffic for the pipeline.
package demo

import (
	"math/rand"
	"sync"
	"time"
)

// Simulator decides how long a fake request takes and whether it fails.
//
// The random source is injected rather than taken from the global rand so that
// tests can pin behaviour: a zero failure rate always succeeds, a rate of one
// always fails, and a fixed seed reproduces a run exactly.
type Simulator struct {
	mu          sync.Mutex
	rng         *rand.Rand
	failureRate float64
	minLatency  time.Duration
	maxLatency  time.Duration
}

// SimulatorConfig configures a Simulator.
type SimulatorConfig struct {
	FailureRate float64
	MinLatency  time.Duration
	MaxLatency  time.Duration
	// Source is optional; when nil a time-seeded source is used.
	Source rand.Source
}

// NewSimulator builds a Simulator. Latency bounds are clamped so a misconfigured
// range degrades to a fixed latency rather than panicking.
func NewSimulator(cfg SimulatorConfig) *Simulator {
	source := cfg.Source
	if source == nil {
		source = rand.NewSource(time.Now().UnixNano())
	}
	if cfg.MinLatency < 0 {
		cfg.MinLatency = 0
	}
	if cfg.MaxLatency < cfg.MinLatency {
		cfg.MaxLatency = cfg.MinLatency
	}

	return &Simulator{
		rng:         rand.New(source),
		failureRate: cfg.FailureRate,
		minLatency:  cfg.MinLatency,
		maxLatency:  cfg.MaxLatency,
	}
}

// ShouldFail reports whether this request should be treated as a failure.
func (s *Simulator) ShouldFail() bool {
	if s.failureRate <= 0 {
		return false
	}
	if s.failureRate >= 1 {
		return true
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rng.Float64() < s.failureRate
}

// Latency returns a duration drawn from the configured range.
func (s *Simulator) Latency() time.Duration {
	spread := s.maxLatency - s.minLatency
	if spread <= 0 {
		return s.minLatency
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.minLatency + time.Duration(s.rng.Int63n(int64(spread)))
}
