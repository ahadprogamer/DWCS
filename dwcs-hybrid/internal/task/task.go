package task

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrTaskNotFound      = errors.New("task not found")
	ErrTaskNotAvailable  = errors.New("task not available: already owned")
	ErrNotOwner          = errors.New("client does not own this task")
	ErrTaskAlreadyExists = errors.New("task already registered")
)

type Status string

const (
	StatusAvailable Status = "available"
	StatusOwned     Status = "owned"
	StatusCompleted Status = "completed"
)

type Task struct {
	ID         string
	Tags       []string
	Hint       []byte
	Status     Status
	OwnerID    string
	LeaseUntil time.Time
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

type LeaseOptions struct {
	Duration time.Duration
}

const DefaultLeaseDuration = 30 * time.Second

type AcquireResult struct {
	Task       Task
	LeaseUntil time.Time
}

type LostEvent struct {
	TaskID  string
	OwnerID string
	Reason  string
}

type AvailableEvent struct {
	TaskID string
	Tags   []string
	Hint   []byte
	Reason string
}

type LostHandler func(LostEvent)
type AvailableHandler func(AvailableEvent)

type Registry struct {
	mu            sync.RWMutex
	tasks         map[string]*Task
	lostHandlers  []LostHandler
	availHandlers []AvailableHandler
	done          chan struct{}
	closed        bool
}

func NewRegistry() *Registry {
	r := &Registry{
		tasks: make(map[string]*Task),
		done:  make(chan struct{}),
	}
	go r.leaseWatcher()
	return r
}

func (r *Registry) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	r.closed = true
	close(r.done)
}

func (r *Registry) OnOwnershipLost(h LostHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lostHandlers = append(r.lostHandlers, h)
}

func (r *Registry) OnAvailable(h AvailableHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.availHandlers = append(r.availHandlers, h)
}

func (r *Registry) Register(id string, tags []string, hint []byte) error {
	r.mu.Lock()
	if _, ok := r.tasks[id]; ok {
		r.mu.Unlock()
		return ErrTaskAlreadyExists
	}
	tagsCopy := append([]string(nil), tags...)
	hintCopy := append([]byte(nil), hint...)
	r.tasks[id] = &Task{
		ID:        id,
		Tags:      tagsCopy,
		Hint:      hintCopy,
		Status:    StatusAvailable,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	handlers := r.availHandlers
	r.mu.Unlock()

	r.fireAvailable(AvailableEvent{TaskID: id, Tags: tagsCopy, Hint: hintCopy, Reason: "registered"}, handlers)
	return nil
}

func (r *Registry) Get(id string) (Task, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.tasks[id]
	if !ok {
		return Task{}, ErrTaskNotFound
	}
	return *t, nil
}

func (r *Registry) ListAvailable(filterTags []string) []Task {
	r.mu.RLock()
	defer r.mu.RUnlock()
	tagSet := toSet(filterTags)
	var out []Task
	for _, t := range r.tasks {
		if t.Status != StatusAvailable {
			continue
		}
		if len(tagSet) == 0 || tagsIntersect(t.Tags, tagSet) {
			out = append(out, *t)
		}
	}
	return out
}

func (r *Registry) Acquire(taskID, ownerID string, opts LeaseOptions) (AcquireResult, error) {
	r.mu.Lock()
	t, ok := r.tasks[taskID]
	if !ok {
		r.mu.Unlock()
		return AcquireResult{}, ErrTaskNotFound
	}

	if t.Status == StatusOwned {
		if !t.LeaseUntil.IsZero() && time.Now().UTC().After(t.LeaseUntil) {

			prevOwner := t.OwnerID
			t.Status = StatusAvailable
			t.OwnerID = ""
			t.LeaseUntil = time.Time{}
			tagsCopy := append([]string(nil), t.Tags...)
			hintCopy := append([]byte(nil), t.Hint...)
			lostHandlers := r.lostHandlers
			availHandlers := r.availHandlers
			r.mu.Unlock()

			for _, h := range lostHandlers {
				h(LostEvent{TaskID: taskID, OwnerID: prevOwner, Reason: "lease_expired"})
			}
			r.fireAvailable(AvailableEvent{TaskID: taskID, Tags: tagsCopy, Hint: hintCopy, Reason: "lease_expired"}, availHandlers)

			r.mu.Lock()
			t = r.tasks[taskID]
			if t == nil {
				r.mu.Unlock()
				return AcquireResult{}, ErrTaskNotFound
			}
		} else {
			r.mu.Unlock()
			return AcquireResult{}, ErrTaskNotAvailable
		}
	}

	dur := opts.Duration
	if dur <= 0 {
		dur = DefaultLeaseDuration
	}
	t.Status = StatusOwned
	t.OwnerID = ownerID
	t.LeaseUntil = time.Now().UTC().Add(dur)
	t.UpdatedAt = time.Now().UTC()
	result := AcquireResult{Task: *t, LeaseUntil: t.LeaseUntil}
	r.mu.Unlock()
	return result, nil
}

func (r *Registry) Release(taskID, ownerID string) error {
	r.mu.Lock()
	t, ok := r.tasks[taskID]
	if !ok {
		r.mu.Unlock()
		return ErrTaskNotFound
	}
	if t.OwnerID != ownerID {
		r.mu.Unlock()
		return ErrNotOwner
	}
	tagsCopy := append([]string(nil), t.Tags...)
	hintCopy := append([]byte(nil), t.Hint...)
	t.Status = StatusAvailable
	t.OwnerID = ""
	t.LeaseUntil = time.Time{}
	t.UpdatedAt = time.Now().UTC()
	handlers := r.availHandlers
	r.mu.Unlock()

	r.fireAvailable(AvailableEvent{TaskID: taskID, Tags: tagsCopy, Hint: hintCopy, Reason: "released"}, handlers)
	return nil
}

func (r *Registry) ReleaseAll(ownerID string) []string {
	r.mu.Lock()
	type release struct {
		id   string
		tags []string
		hint []byte
	}
	var released []release
	for _, t := range r.tasks {
		if t.Status == StatusOwned && t.OwnerID == ownerID {
			released = append(released, release{
				id:   t.ID,
				tags: append([]string(nil), t.Tags...),
				hint: append([]byte(nil), t.Hint...),
			})
			t.Status = StatusAvailable
			t.OwnerID = ""
			t.LeaseUntil = time.Time{}
			t.UpdatedAt = time.Now().UTC()
		}
	}
	availHandlers := r.availHandlers
	lostHandlers := r.lostHandlers
	r.mu.Unlock()

	ids := make([]string, len(released))
	for i, rel := range released {
		ids[i] = rel.id
		r.fireAvailable(AvailableEvent{TaskID: rel.id, Tags: rel.tags, Hint: rel.hint, Reason: "disconnected"}, availHandlers)
		for _, h := range lostHandlers {
			h(LostEvent{TaskID: rel.id, OwnerID: ownerID, Reason: "disconnected"})
		}
	}
	return ids
}

func (r *Registry) MarkCompleted(taskID, ownerID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.tasks[taskID]
	if !ok {
		return ErrTaskNotFound
	}
	if t.OwnerID != ownerID {
		return ErrNotOwner
	}
	t.Status = StatusCompleted
	t.OwnerID = ""
	t.LeaseUntil = time.Time{}
	t.UpdatedAt = time.Now().UTC()
	return nil
}

func (r *Registry) leaseWatcher() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-r.done:
			return
		case <-ticker.C:
			r.evictExpired()
		}
	}
}

func (r *Registry) evictExpired() {
	now := time.Now().UTC()
	type expired struct {
		id, owner string
		tags      []string
		hint      []byte
	}
	var expiredTasks []expired

	r.mu.Lock()
	for _, t := range r.tasks {
		if t.Status == StatusOwned && !t.LeaseUntil.IsZero() && now.After(t.LeaseUntil) {
			expiredTasks = append(expiredTasks, expired{
				id:    t.ID,
				owner: t.OwnerID,
				tags:  append([]string(nil), t.Tags...),
				hint:  append([]byte(nil), t.Hint...),
			})
			t.Status = StatusAvailable
			t.OwnerID = ""
			t.LeaseUntil = time.Time{}
			t.UpdatedAt = time.Now().UTC()
		}
	}
	availHandlers := r.availHandlers
	lostHandlers := r.lostHandlers
	r.mu.Unlock()

	for _, e := range expiredTasks {
		for _, h := range lostHandlers {
			h(LostEvent{TaskID: e.id, OwnerID: e.owner, Reason: "lease_expired"})
		}
		r.fireAvailable(AvailableEvent{TaskID: e.id, Tags: e.tags, Hint: e.hint, Reason: "lease_expired"}, availHandlers)
	}
}

func (r *Registry) fireAvailable(evt AvailableEvent, handlers []AvailableHandler) {
	for _, h := range handlers {
		func() {
			defer func() { _ = recover() }()
			h(evt)
		}()
	}
}

func toSet(tags []string) map[string]struct{} {
	s := make(map[string]struct{}, len(tags))
	for _, t := range tags {
		s[t] = struct{}{}
	}
	return s
}

func tagsIntersect(taskTags []string, filterSet map[string]struct{}) bool {
	for _, t := range taskTags {
		if _, ok := filterSet[t]; ok {
			return true
		}
	}
	return false
}
