package gossip

import (
	"sync"
	"time"

	"github.com/dwcs/backend/internal/peering"
	"github.com/dwcs/backend/internal/protocol"
)

type Gossip struct {
	mgr      *peering.Manager
	seenMu   sync.Mutex
	seen     map[string]time.Time
	maxAge   time.Duration
	stopChan chan struct{}
}

func New(mgr *peering.Manager) *Gossip {
	return &Gossip{
		mgr:      mgr,
		seen:     make(map[string]time.Time),
		maxAge:   5 * time.Minute,
		stopChan: make(chan struct{}),
	}
}

func (g *Gossip) Start() {
	go g.gcLoop()
}

func (g *Gossip) Stop() {
	close(g.stopChan)
}

func (g *Gossip) gcLoop() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-g.stopChan:
			return
		case <-ticker.C:
			g.gc()
		}
	}
}

func (g *Gossip) gc() {
	cutoff := time.Now().Add(-g.maxAge)
	g.seenMu.Lock()
	for id, t := range g.seen {
		if t.Before(cutoff) {
			delete(g.seen, id)
		}
	}
	g.seenMu.Unlock()
}

func (g *Gossip) ShouldForward(env protocol.Envelope) bool {
	id := messageID(env)
	g.seenMu.Lock()
	defer g.seenMu.Unlock()
	if _, ok := g.seen[id]; ok {
		return false
	}
	g.seen[id] = time.Now()
	return true
}

func (g *Gossip) Broadcast(msgType protocol.MessageType, requestID string, payload any) error {
	msg, err := protocol.Encode(msgType, requestID, payload)
	if err != nil {
		return err
	}
	g.mgr.Broadcast(msg)
	return nil
}

func (g *Gossip) BroadcastExcept(msgType protocol.MessageType, requestID string, payload any, exceptPeerID string) error {
	msg, err := protocol.Encode(msgType, requestID, payload)
	if err != nil {
		return err
	}
	g.mgr.BroadcastExcept(msg, exceptPeerID)
	return nil
}

func messageID(env protocol.Envelope) string {
	if env.RequestID != "" {
		return env.RequestID
	}
	return string(env.Type) + ":" + string(env.Payload)
}
