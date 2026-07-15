package world

import (
	"encoding/json"
	"errors"
	"sync"
	"time"
)

var (
	ErrObjectNotFound  = errors.New("object not found")
	ErrVersionConflict = errors.New("version conflict: submitted version is stale")
)

type Object struct {
	ID        string
	Data      json.RawMessage
	Version   uint64
	Tags      []string
	Meta      json.RawMessage
	UpdatedAt time.Time
}

type UpdateRequest struct {
	ObjectID        string
	Data            json.RawMessage
	ExpectedVersion uint64
	Tags            []string
	Meta            json.RawMessage
}

type ChangeKind string

const (
	ChangeCreated ChangeKind = "created"
	ChangeUpdated ChangeKind = "updated"
	ChangeDeleted ChangeKind = "deleted"
)

type ChangeEvent struct {
	Kind        ChangeKind
	Object      Object
	PrevVersion uint64
}

type ChangeHandler func(event ChangeEvent)

type World struct {
	mu       sync.RWMutex
	objects  map[string]*Object
	handlers []ChangeHandler
}

func New() *World {
	return &World{objects: make(map[string]*Object)}
}

func (w *World) OnChange(h ChangeHandler) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.handlers = append(w.handlers, h)
}

func (w *World) Get(id string) (Object, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	obj, ok := w.objects[id]
	if !ok {
		return Object{}, ErrObjectNotFound
	}
	return cloneObject(obj), nil
}

func (w *World) List(filterTags []string) []Object {
	w.mu.RLock()
	defer w.mu.RUnlock()
	tagSet := toSet(filterTags)
	out := make([]Object, 0, len(w.objects))
	for _, obj := range w.objects {
		if len(tagSet) == 0 || tagsIntersect(obj.Tags, tagSet) {
			out = append(out, cloneObject(obj))
		}
	}
	return out
}

func (w *World) Apply(req UpdateRequest) (Object, error) {
	w.mu.Lock()
	existing, exists := w.objects[req.ObjectID]

	if req.ExpectedVersion > 0 {
		if !exists {
			w.mu.Unlock()
			return Object{}, ErrObjectNotFound
		}
		if existing.Version != req.ExpectedVersion {
			w.mu.Unlock()
			return Object{}, ErrVersionConflict
		}
	}

	prevVersion := uint64(0)
	nextVersion := uint64(1)
	kind := ChangeCreated
	if exists {
		prevVersion = existing.Version
		nextVersion = existing.Version + 1
		kind = ChangeUpdated
	}

	tags := copyTags(req.Tags)
	if tags == nil && exists {
		tags = copyTags(existing.Tags)
	}
	data := copyBytes(req.Data)
	if data == nil && exists {
		data = copyBytes(existing.Data)
	}
	meta := copyBytes(req.Meta)
	if meta == nil && exists {
		meta = copyBytes(existing.Meta)
	}

	updated := &Object{
		ID:        req.ObjectID,
		Data:      data,
		Version:   nextVersion,
		Tags:      tags,
		Meta:      meta,
		UpdatedAt: time.Now().UTC(),
	}
	w.objects[req.ObjectID] = updated
	handlers := w.handlers
	w.mu.Unlock()

	w.fire(ChangeEvent{Kind: kind, Object: *updated, PrevVersion: prevVersion}, handlers)
	return *updated, nil
}

func (w *World) Delete(id string) bool {
	w.mu.Lock()
	existing, ok := w.objects[id]
	if !ok {
		w.mu.Unlock()
		return false
	}
	delete(w.objects, id)
	handlers := w.handlers
	w.mu.Unlock()

	w.fire(ChangeEvent{
		Kind:        ChangeDeleted,
		Object:      cloneObject(existing),
		PrevVersion: existing.Version,
	}, handlers)
	return true
}

func (w *World) Snapshot() []Object {
	w.mu.RLock()
	defer w.mu.RUnlock()
	out := make([]Object, 0, len(w.objects))
	for _, obj := range w.objects {
		out = append(out, cloneObject(obj))
	}
	return out
}

func (w *World) fire(evt ChangeEvent, handlers []ChangeHandler) {
	for _, h := range handlers {
		func() {
			defer func() { _ = recover() }()
			h(evt)
		}()
	}
}

func cloneObject(o *Object) Object {
	return Object{
		ID:        o.ID,
		Data:      copyBytes(o.Data),
		Version:   o.Version,
		Tags:      copyTags(o.Tags),
		Meta:      copyBytes(o.Meta),
		UpdatedAt: o.UpdatedAt,
	}
}

func copyTags(in []string) []string {
	if in == nil {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}

func copyBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

func toSet(tags []string) map[string]struct{} {
	s := make(map[string]struct{}, len(tags))
	for _, t := range tags {
		s[t] = struct{}{}
	}
	return s
}

func tagsIntersect(objTags []string, filterSet map[string]struct{}) bool {
	for _, t := range objTags {
		if _, ok := filterSet[t]; ok {
			return true
		}
	}
	return false
}
