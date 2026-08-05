package peering

import (
        "sync"
)

type fanInManager struct {
        a        PeeringManager
        b        PeeringManager
        merged   chan PeerEvent
        stopOnce sync.Once
        stopChan chan struct{}
}

func NewFanInManager(a, b PeeringManager) PeeringManager {
        m := &fanInManager{
                a:        a,
                b:        b,
                merged:   make(chan PeerEvent, 2048),
                stopChan: make(chan struct{}),
        }
        go m.fanIn(a.Events())
        go m.fanIn(b.Events())
        return m
}

func (m *fanInManager) fanIn(ch <-chan PeerEvent) {
        for {
                select {
                case <-m.stopChan:
                        return
                case evt, ok := <-ch:
                        if !ok {
                                return
                        }
                        select {
                        case m.merged <- evt:
                        default:
                        }
                }
        }
}

func (m *fanInManager) Listen(addr string) error {
        return nil
}

func (m *fanInManager) Dial(addr string) error {
        errA := m.a.Dial(addr)
        errB := m.b.Dial(addr)
        if errA != nil {
                return errA
        }
        return errB
}

func (m *fanInManager) Broadcast(msg []byte) {
        m.a.Broadcast(msg)
        m.b.Broadcast(msg)
}

func (m *fanInManager) BroadcastExcept(msg []byte, exceptPeerID string) {
        m.a.BroadcastExcept(msg, exceptPeerID)
        m.b.BroadcastExcept(msg, exceptPeerID)
}

func (m *fanInManager) Events() <-chan PeerEvent {
        return m.merged
}

func (m *fanInManager) PeerCount() int {
        return m.a.PeerCount() + m.b.PeerCount()
}

// Addr returns the address of the first underlying manager that is listening.
func (m *fanInManager) Addr() string {
        if a := m.a.Addr(); a != "" {
                return a
        }
        return m.b.Addr()
}

func (m *fanInManager) Stop() {
        m.stopOnce.Do(func() {
                close(m.stopChan)
                m.a.Stop()
                m.b.Stop()
        })
}
