package projector

import (
	"sync"
	"time"

	"github.com/dwcs/backend/internal/metrics"
	"github.com/dwcs/backend/internal/protocol"
	"github.com/dwcs/backend/internal/session"
	"github.com/dwcs/backend/internal/world"
)

type Projector struct {
	world   *world.World
	manager *session.Manager
	rate    time.Duration
	metrics *metrics.Recorder

	mu       sync.Mutex
	seen     map[string]map[string]uint64
	stopChan chan struct{}
	stopped  bool
}

func New(w *world.World, m *session.Manager, rate time.Duration, mr *metrics.Recorder) *Projector {
	if rate <= 0 {
		rate = 100 * time.Millisecond
	}
	return &Projector{
		world:    w,
		manager:  m,
		rate:     rate,
		metrics:  mr,
		seen:     make(map[string]map[string]uint64),
		stopChan: make(chan struct{}),
	}
}

func (p *Projector) Start() {
	go p.loop()
}

func (p *Projector) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return
	}
	p.stopped = true
	close(p.stopChan)
}

func (p *Projector) ForgetSession(sessionID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.seen, sessionID)
}

func (p *Projector) loop() {
	ticker := time.NewTicker(p.rate)
	defer ticker.Stop()
	for {
		select {
		case <-p.stopChan:
			return
		case <-ticker.C:
			p.tick()
		}
	}
}

func (p *Projector) tick() {

	all := p.world.List(nil)

	deliveredCount := 0
	objectCount := 0
	p.manager.Each(func(s *session.Session) {
		diffs := p.computeDiffs(s, all)
		if len(diffs) == 0 {
			return
		}
		payload := protocol.WorldUpdatePayload{Objects: diffs}
		msg, err := protocol.Encode(protocol.MsgWorldUpdate, "", payload)
		if err != nil {
			return
		}
		if s.Send(msg) == nil {
			deliveredCount++
			if objectCount == 0 {
				objectCount = len(diffs)
			}
		}
	})

	if deliveredCount > 0 && p.metrics.Enabled() {
		p.metrics.Broadcast(deliveredCount, objectCount)
	}
}

func (p *Projector) computeDiffs(s *session.Session, all []world.Object) []protocol.ObjectDiff {
	p.mu.Lock()
	seen, ok := p.seen[s.ID]
	if !ok {
		seen = make(map[string]uint64)
		p.seen[s.ID] = seen
	}
	p.mu.Unlock()

	var diffs []protocol.ObjectDiff
	for _, obj := range all {

		if !sessionMatchesAnyTag(s, obj.Tags) {
			continue
		}
		last := seen[obj.ID]
		if obj.Version > last {
			diffs = append(diffs, protocol.ObjectDiff{
				ObjectID: obj.ID,
				Data:     obj.Data,
				Version:  obj.Version,
				Meta:     obj.Meta,
			})
			seen[obj.ID] = obj.Version
		}
	}
	return diffs
}

func sessionMatchesAnyTag(s *session.Session, objTags []string) bool {
	if s.HasTag("*") {
		return true
	}
	for _, t := range objTags {
		if s.HasTag(t) {
			return true
		}
	}
	return false
}
