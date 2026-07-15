package session

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrSessionNotFound = errors.New("session not found")
	ErrSessionClosed   = errors.New("session is closed")
)

type Session struct {
	ID          string
	ConnectedAt time.Time

	mu     sync.RWMutex
	tags   map[string]struct{}
	outbox chan []byte
	closed bool
}

const outboxBuffer = 512

func newSession(id string) *Session {
	return &Session{
		ID:          id,
		ConnectedAt: time.Now().UTC(),
		tags:        make(map[string]struct{}),
		outbox:      make(chan []byte, outboxBuffer),
	}
}

func (s *Session) Subscribe(tags []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range tags {
		s.tags[t] = struct{}{}
	}
}

func (s *Session) Unsubscribe(tags []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range tags {
		delete(s.tags, t)
	}
}

func (s *Session) HasTag(tag string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.tags[tag]
	return ok
}

func (s *Session) Tags() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.tags))
	for t := range s.tags {
		out = append(out, t)
	}
	return out
}

func (s *Session) Send(msg []byte) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return ErrSessionClosed
	}
	select {
	case s.outbox <- msg:
	default:

	}
	return nil
}

func (s *Session) Outbox() <-chan []byte {
	return s.outbox
}

func (s *Session) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true

}

func (s *Session) IsClosed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.closed
}

type Manager struct {
	mu       sync.RWMutex
	sessions map[string]*Session
}

func NewManager() *Manager {
	return &Manager{sessions: make(map[string]*Session)}
}

func (m *Manager) Add(id string) *Session {
	s := newSession(id)
	m.mu.Lock()
	m.sessions[id] = s
	m.mu.Unlock()
	return s
}

func (m *Manager) Get(id string) (*Session, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.sessions[id]
	if !ok {
		return nil, ErrSessionNotFound
	}
	return s, nil
}

func (m *Manager) Remove(id string) {
	m.mu.Lock()
	s, ok := m.sessions[id]
	if ok {
		delete(m.sessions, id)
	}
	m.mu.Unlock()
	if ok {
		s.Close()
	}
}

func (m *Manager) Each(fn func(*Session)) {
	m.mu.RLock()
	snap := make([]*Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		snap = append(snap, s)
	}
	m.mu.RUnlock()
	for _, s := range snap {
		fn(s)
	}
}

func (m *Manager) Broadcast(tags []string, msg []byte) {
	m.Each(func(s *Session) {
		if len(tags) == 0 || sessionMatchesTags(s, tags) {
			_ = s.Send(msg)
		}
	})
}

func (m *Manager) BroadcastAll(msg []byte) { m.Broadcast(nil, msg) }

func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.sessions)
}

func sessionMatchesTags(s *Session, tags []string) bool {
	for _, t := range tags {
		if s.HasTag(t) {
			return true
		}
	}
	return false
}
