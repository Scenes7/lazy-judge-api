package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ─── Result Entry ─────────────────────────────────────────────────────────────

// resultEntry holds the stream of WSEvents for one submission.
// It is safe for concurrent reads/writes from the Lambda goroutine and
// any number of WebSocket goroutines.
type resultEntry struct {
	events []WSEvent
	done   bool
	mu     sync.Mutex
	notify chan struct{} // replaced on every new event; closing it wakes waiters
}

func newEntry() *resultEntry {
	return &resultEntry{notify: make(chan struct{})}
}

// appendEvent appends ev and wakes all waiting WebSocket goroutines.
func (e *resultEntry) appendEvent(ev WSEvent) {
	e.mu.Lock()
	e.events = append(e.events, ev)
	old := e.notify
	e.notify = make(chan struct{})
	e.mu.Unlock()
	close(old)
}

// finalise marks the entry complete and does one final wake-up.
func (e *resultEntry) finalise() {
	e.mu.Lock()
	e.done = true
	old := e.notify
	e.notify = make(chan struct{})
	e.mu.Unlock()
	close(old)
}

// ─── Result Store ─────────────────────────────────────────────────────────────

var (
	resultsMu sync.RWMutex
	results   = make(map[string]*resultEntry)
)

func storeEntry(id string, entry *resultEntry) {
	resultsMu.Lock()
	results[id] = entry
	resultsMu.Unlock()
}

func getEntry(id string) (*resultEntry, bool) {
	resultsMu.RLock()
	e, ok := results[id]
	resultsMu.RUnlock()
	return e, ok
}

// newSubmissionID generates a unique ID that encodes whether this is an intro
// or sprint submission and, for sprints, the slot position within the sprint.
//
//   Intro:  intro_{timestamp_ms}_{counter}
//   Sprint: sprint_{sprintID}_{slotIndex}of{sprintSize}_{counter}
//
// The "Xof Y" suffix lets invokeLambda detect the last slot (X == Y-1) without
// any cross-submission coordination.
var subCounter atomic.Uint64

func newSubmissionID(req SubmitRequest) string {
	n := subCounter.Add(1)
	if req.SprintID != "" && req.SprintSize > 0 {
		return fmt.Sprintf("sprint_%s_%dof%d_%d", req.SprintID, req.SlotIndex, req.SprintSize, n)
	}
	return fmt.Sprintf("intro_%d_%d", time.Now().UnixMilli(), n)
}
