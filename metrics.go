package metrics

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Recorder struct {
	mu        sync.Mutex
	enabled   bool
	startedAt time.Time

	totalClaims      atomic.Uint64
	totalCompletions atomic.Uint64
	totalRejections  atomic.Uint64
	totalEvictions   atomic.Uint64
	totalReleases    atomic.Uint64
	totalBroadcasts  atomic.Uint64

	tasksMu sync.Mutex
	tasks   map[string]*TaskStats

	sessionsMu sync.Mutex
	sessions   map[string]*SessionStats
}

type TaskStats struct {
	Claims      uint64
	Completions uint64
	Rejections  uint64
	Evictions   uint64
	Releases    uint64

	totalComputeMs atomic.Uint64

	inFlightMu sync.Mutex
	inFlight   map[string]time.Time
}

type SessionStats struct {
	Claims       uint64
	Completions  uint64
	Rejections   uint64
	Evictions    uint64
	CurrentOwned uint64
}

func New(enabled bool) *Recorder {
	return &Recorder{
		enabled:   enabled,
		startedAt: time.Now(),
		tasks:     make(map[string]*TaskStats),
		sessions:  make(map[string]*SessionStats),
	}
}

func (r *Recorder) Enabled() bool {
	return r != nil && r.enabled
}

func (r *Recorder) TaskRegistered(taskID string, tags []string) {
	if !r.Enabled() {
		return
	}
	r.getOrCreateTask(taskID)
	r.logEvent("task_registered", map[string]any{
		"task": taskID,
		"tags": tags,
	})
}

func (r *Recorder) TaskClaimed(taskID, ownerID string, leaseDuration time.Duration) {
	if !r.Enabled() {
		return
	}
	r.totalClaims.Add(1)
	ts := r.getOrCreateTask(taskID)
	ts.Claims++
	ts.inFlightMu.Lock()
	if ts.inFlight == nil {
		ts.inFlight = make(map[string]time.Time)
	}
	ts.inFlight[ownerID] = time.Now()
	ts.inFlightMu.Unlock()

	ss := r.getOrCreateSession(ownerID)
	ss.Claims++
	ss.CurrentOwned++

	r.logEvent("task_claimed", map[string]any{
		"task":  taskID,
		"owner": ownerID,
		"lease": leaseDuration.String(),
	})
}

func (r *Recorder) TaskCompleted(taskID, ownerID string) {
	if !r.Enabled() {
		return
	}
	r.totalCompletions.Add(1)
	ts := r.getOrCreateTask(taskID)
	ts.Completions++

	ts.inFlightMu.Lock()
	startedAt, ok := ts.inFlight[ownerID]
	if ok {
		delete(ts.inFlight, ownerID)
	}
	ts.inFlightMu.Unlock()

	fields := map[string]any{
		"task":  taskID,
		"owner": ownerID,
	}
	if ok {
		ms := time.Since(startedAt).Milliseconds()
		ts.totalComputeMs.Add(uint64(ms))
		fields["compute_ms"] = ms
	}
	r.logEvent("task_completed", fields)

	ss := r.getOrCreateSession(ownerID)
	ss.Completions++
}

func (r *Recorder) TaskRejected(taskID, ownerID, reason string) {
	if !r.Enabled() {
		return
	}
	r.totalRejections.Add(1)
	ts := r.getOrCreateTask(taskID)
	ts.Rejections++

	ts.inFlightMu.Lock()
	delete(ts.inFlight, ownerID)
	ts.inFlightMu.Unlock()

	ss := r.getOrCreateSession(ownerID)
	ss.Rejections++

	r.logEvent("task_rejected", map[string]any{
		"task":   taskID,
		"owner":  ownerID,
		"reason": reason,
	})
}

func (r *Recorder) TaskReleased(taskID, ownerID string) {
	if !r.Enabled() {
		return
	}
	r.totalReleases.Add(1)
	ts := r.getOrCreateTask(taskID)
	ts.Releases++
	ts.inFlightMu.Lock()
	delete(ts.inFlight, ownerID)
	ts.inFlightMu.Unlock()

	ss := r.getOrCreateSession(ownerID)
	if ss.CurrentOwned > 0 {
		ss.CurrentOwned--
	}

	r.logEvent("task_released", map[string]any{
		"task":  taskID,
		"owner": ownerID,
	})
}

func (r *Recorder) TaskEvicted(taskID, ownerID, reason string) {
	if !r.Enabled() {
		return
	}
	r.totalEvictions.Add(1)
	ts := r.getOrCreateTask(taskID)
	ts.Evictions++
	ts.inFlightMu.Lock()
	delete(ts.inFlight, ownerID)
	ts.inFlightMu.Unlock()

	ss := r.getOrCreateSession(ownerID)
	ss.Evictions++
	if ss.CurrentOwned > 0 {
		ss.CurrentOwned--
	}

	r.logEvent("task_evicted", map[string]any{
		"task":   taskID,
		"owner":  ownerID,
		"reason": reason,
	})
}

func (r *Recorder) WorldUpdated(objectID string, prevVersion, newVersion uint64) {
	if !r.Enabled() {
		return
	}
	r.logEvent("world_updated", map[string]any{
		"object":   objectID,
		"prev_ver": prevVersion,
		"new_ver":  newVersion,
	})
}

func (r *Recorder) Broadcast(subscriberCount int, objectCount int) {
	if !r.Enabled() {
		return
	}
	r.totalBroadcasts.Add(1)
	r.logEvent("broadcast", map[string]any{
		"subscribers": subscriberCount,
		"objects":     objectCount,
	})
}

func (r *Recorder) SessionConnected(sessionID string) {
	if !r.Enabled() {
		return
	}
	r.getOrCreateSession(sessionID)
	r.logEvent("session_connected", map[string]any{
		"session": sessionID,
	})
}

func (r *Recorder) SessionDisconnected(sessionID string, tasksReleased int) {
	if !r.Enabled() {
		return
	}
	r.logEvent("session_disconnected", map[string]any{
		"session":        sessionID,
		"tasks_released": tasksReleased,
	})
	r.sessionsMu.Lock()
	delete(r.sessions, sessionID)
	r.sessionsMu.Unlock()
}

type Snapshot struct {
	Uptime     string                 `json:"uptime"`
	Enabled    bool                   `json:"enabled"`
	Totals     Totals                 `json:"totals"`
	PerTask    map[string]TaskSnap    `json:"per_task"`
	PerSession map[string]SessionSnap `json:"per_session"`
}

type Totals struct {
	Claims      uint64 `json:"claims"`
	Completions uint64 `json:"completions"`
	Rejections  uint64 `json:"rejections"`
	Evictions   uint64 `json:"evictions"`
	Releases    uint64 `json:"releases"`
	Broadcasts  uint64 `json:"broadcasts"`
}

type TaskSnap struct {
	Claims       uint64 `json:"claims"`
	Completions  uint64 `json:"completions"`
	Rejections   uint64 `json:"rejections"`
	Evictions    uint64 `json:"evictions"`
	Releases     uint64 `json:"releases"`
	AvgComputeMs uint64 `json:"avg_compute_ms"`
}

type SessionSnap struct {
	Claims       uint64 `json:"claims"`
	Completions  uint64 `json:"completions"`
	Rejections   uint64 `json:"rejections"`
	Evictions    uint64 `json:"evictions"`
	CurrentOwned uint64 `json:"current_owned"`
}

func (r *Recorder) Snapshot() Snapshot {
	if r == nil {
		return Snapshot{Enabled: false}
	}
	snap := Snapshot{
		Uptime:  time.Since(r.startedAt).Round(time.Second).String(),
		Enabled: r.enabled,
		Totals: Totals{
			Claims:      r.totalClaims.Load(),
			Completions: r.totalCompletions.Load(),
			Rejections:  r.totalRejections.Load(),
			Evictions:   r.totalEvictions.Load(),
			Releases:    r.totalReleases.Load(),
			Broadcasts:  r.totalBroadcasts.Load(),
		},
		PerTask:    make(map[string]TaskSnap),
		PerSession: make(map[string]SessionSnap),
	}

	r.tasksMu.Lock()
	for id, ts := range r.tasks {
		var avg uint64
		if ts.Completions > 0 {
			avg = ts.totalComputeMs.Load() / ts.Completions
		}
		snap.PerTask[id] = TaskSnap{
			Claims:       ts.Claims,
			Completions:  ts.Completions,
			Rejections:   ts.Rejections,
			Evictions:    ts.Evictions,
			Releases:     ts.Releases,
			AvgComputeMs: avg,
		}
	}
	r.tasksMu.Unlock()

	r.sessionsMu.Lock()
	for id, ss := range r.sessions {
		snap.PerSession[id] = SessionSnap{
			Claims:       ss.Claims,
			Completions:  ss.Completions,
			Rejections:   ss.Rejections,
			Evictions:    ss.Evictions,
			CurrentOwned: ss.CurrentOwned,
		}
	}
	r.sessionsMu.Unlock()

	return snap
}

func (r *Recorder) HTTPHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(r.Snapshot())
	}
}

func (r *Recorder) getOrCreateTask(taskID string) *TaskStats {
	r.tasksMu.Lock()
	defer r.tasksMu.Unlock()
	ts, ok := r.tasks[taskID]
	if !ok {
		ts = &TaskStats{}
		r.tasks[taskID] = ts
	}
	return ts
}

func (r *Recorder) getOrCreateSession(sessionID string) *SessionStats {
	r.sessionsMu.Lock()
	defer r.sessionsMu.Unlock()
	ss, ok := r.sessions[sessionID]
	if !ok {
		ss = &SessionStats{}
		r.sessions[sessionID] = ss
	}
	return ss
}

func (r *Recorder) logEvent(kind string, fields map[string]any) {
	parts := make([]string, 0, len(fields)+1)
	parts = append(parts, "["+kind+"]")
	for _, k := range orderedKeys(fields) {
		parts = append(parts, k+"="+formatValue(fields[k]))
	}

	log.Println(strings.Join(parts, " "))
}

var preferredOrder = []string{
	"task", "owner", "session", "object", "reason",
	"compute_ms", "lease", "prev_ver", "new_ver",
	"subscribers", "objects", "tasks_released", "tags",
}

func orderedKeys(m map[string]any) []string {
	seen := make(map[string]bool, len(m))
	out := make([]string, 0, len(m))
	for _, k := range preferredOrder {
		if _, ok := m[k]; ok {
			out = append(out, k)
			seen[k] = true
		}
	}
	for k := range m {
		if !seen[k] {
			out = append(out, k)
		}
	}
	return out
}

func formatValue(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case uint64:
		return strconv.FormatUint(x, 10)
	case int:
		return strconv.Itoa(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case []string:
		out := ""
		for i, s := range x {
			if i > 0 {
				out += ","
			}
			out += s
		}
		return out
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}
