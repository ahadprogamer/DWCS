package peering

import (
        "encoding/json"
        "fmt"
        "net"
        "sync"
        "sync/atomic"
        "time"

        "github.com/dwcs/backend/internal/protocol"
)

const (
        udpMaxDatagramSize = 65507
        udpPeerTimeout     = 30 * time.Second
        udpHeartbeatEvery  = 10 * time.Second
)

type udpPeer struct {
        id       string
        addr     *net.UDPAddr
        lastSeen atomic.Int64
        outbox   chan []byte
        closed   atomic.Bool
}

func newUDPPeer(id string, addr *net.UDPAddr) *udpPeer {
        p := &udpPeer{
                id:     id,
                addr:   addr,
                outbox: make(chan []byte, 256),
        }
        p.lastSeen.Store(time.Now().UnixNano())
        return p
}

func (p *udpPeer) touch() {
        p.lastSeen.Store(time.Now().UnixNano())
}

func (p *udpPeer) isExpired() bool {
        last := time.Unix(0, p.lastSeen.Load())
        return time.Since(last) > udpPeerTimeout
}

type UDPManager struct {
        conn     *net.UDPConn
        mu       sync.RWMutex
        peers    map[string]*udpPeer
        addrToPeer map[string]*udpPeer
        events   chan PeerEvent
        stopOnce sync.Once
        stopChan chan struct{}
        wg       sync.WaitGroup
        counter  atomic.Uint64
}

func NewUDPManager() *UDPManager {
        return &UDPManager{
                peers:      make(map[string]*udpPeer),
                addrToPeer: make(map[string]*udpPeer),
                events:     make(chan PeerEvent, 1024),
                stopChan:   make(chan struct{}),
        }
}

func (m *UDPManager) Events() <-chan PeerEvent {
        return m.events
}

func (m *UDPManager) PeerCount() int {
        m.mu.RLock()
        defer m.mu.RUnlock()
        return len(m.peers)
}

func (m *UDPManager) Listen(addr string) error {
        udpAddr, err := net.ResolveUDPAddr("udp", addr)
        if err != nil {
                return fmt.Errorf("resolve %s: %w", addr, err)
        }
        conn, err := net.ListenUDP("udp", udpAddr)
        if err != nil {
                return fmt.Errorf("listen udp %s: %w", addr, err)
        }
        m.conn = conn
        m.wg.Add(1)
        go m.readLoop()
        m.wg.Add(1)
        go m.writeLoop()
        m.wg.Add(1)
        go m.timeoutLoop()
        return nil
}

// Addr returns the address the manager is listening on, or empty if not started.
func (m *UDPManager) Addr() string {
        m.mu.RLock()
        defer m.mu.RUnlock()
        if m.conn == nil {
                return ""
        }
        return m.conn.LocalAddr().String()
}

func (m *UDPManager) Dial(addr string) error {
        udpAddr, err := net.ResolveUDPAddr("udp", addr)
        if err != nil {
                return fmt.Errorf("resolve %s: %w", addr, err)
        }
        m.mu.Lock()
        key := udpAddr.String()
        if _, exists := m.addrToPeer[key]; exists {
                m.mu.Unlock()
                return nil
        }
        p := m.newPeer(udpAddr)
        m.mu.Unlock()
        m.emit(PeerEvent{Kind: EventPeerConnected, PeerID: p.id})
        return nil
}

func (m *UDPManager) newPeer(addr *net.UDPAddr) *udpPeer {
        m.counter.Add(1)
        id := fmt.Sprintf("udp-peer-%d-%d", time.Now().UnixNano(), m.counter.Load())
        p := newUDPPeer(id, addr)
        m.peers[id] = p
        m.addrToPeer[addr.String()] = p
        return p
}

func (m *UDPManager) readLoop() {
        defer m.wg.Done()
        buf := make([]byte, udpMaxDatagramSize)
        for {
                select {
                case <-m.stopChan:
                        return
                default:
                }
                _ = m.conn.SetReadDeadline(time.Now().Add(1 * time.Second))
                n, addr, err := m.conn.ReadFromUDP(buf)
                if err != nil {
                        if ne, ok := err.(net.Error); ok && ne.Timeout() {
                                continue
                        }
                        select {
                        case <-m.stopChan:
                                return
                        default:
                                continue
                        }
                }
                data := make([]byte, n)
                copy(data, buf[:n])
                m.handleDatagram(addr, data)
        }
}

func (m *UDPManager) handleDatagram(addr *net.UDPAddr, data []byte) {
        key := addr.String()
        m.mu.Lock()
        p, exists := m.addrToPeer[key]
        if !exists {
                p = m.newPeer(addr)
                m.mu.Unlock()
                m.emit(PeerEvent{Kind: EventPeerConnected, PeerID: p.id})
        } else {
                m.mu.Unlock()
        }
        p.touch()

        var env protocol.Envelope
        if err := json.Unmarshal(data, &env); err != nil {
                return
        }
        if env.Type == protocol.MsgPong {
                return
        }
        if env.Type == protocol.MsgPing {
                pong, err := protocol.Encode(protocol.MsgPong, env.RequestID, nil)
                if err == nil {
                        _ = m.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
                        _, _ = m.conn.WriteToUDP(pong, addr)
                }
                return
        }
        m.emit(PeerEvent{Kind: EventPeerMessage, PeerID: p.id, Env: env})
}

func (m *UDPManager) writeLoop() {
        defer m.wg.Done()
        ticker := time.NewTicker(100 * time.Millisecond)
        defer ticker.Stop()
        for {
                select {
                case <-m.stopChan:
                        return
                case <-ticker.C:
                        m.flushOutboxes()
                }
        }
}

func (m *UDPManager) flushOutboxes() {
        m.mu.RLock()
        peers := make([]*udpPeer, 0, len(m.peers))
        for _, p := range m.peers {
                peers = append(peers, p)
        }
        m.mu.RUnlock()

        for _, p := range peers {
                for {
                        select {
                        case msg := <-p.outbox:
                                if m.conn != nil {
                                        _ = m.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
                                        _, _ = m.conn.WriteToUDP(msg, p.addr)
                                }
                        default:
                                goto next
                        }
                }
        next:
        }
}

func (m *UDPManager) timeoutLoop() {
        defer m.wg.Done()
        ticker := time.NewTicker(5 * time.Second)
        heartbeat := time.NewTicker(udpHeartbeatEvery)
        defer ticker.Stop()
        defer heartbeat.Stop()
        for {
                select {
                case <-m.stopChan:
                        return
                case <-ticker.C:
                        m.evictExpired()
                case <-heartbeat.C:
                        m.sendHeartbeats()
                }
        }
}

func (m *UDPManager) evictExpired() {
        m.mu.Lock()
        var evicted []*udpPeer
        for _, p := range m.peers {
                if p.isExpired() {
                        evicted = append(evicted, p)
                }
        }
        for _, p := range evicted {
                delete(m.peers, p.id)
                delete(m.addrToPeer, p.addr.String())
        }
        m.mu.Unlock()
        for _, p := range evicted {
                m.emit(PeerEvent{Kind: EventPeerClosed, PeerID: p.id})
        }
}

func (m *UDPManager) sendHeartbeats() {
        ping, err := protocol.Encode(protocol.MsgPing, "", nil)
        if err != nil {
                return
        }
        m.mu.RLock()
        peers := make([]*udpPeer, 0, len(m.peers))
        for _, p := range m.peers {
                peers = append(peers, p)
        }
        m.mu.RUnlock()
        for _, p := range peers {
                if m.conn != nil {
                        _ = m.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
                        _, _ = m.conn.WriteToUDP(ping, p.addr)
                }
        }
}

func (m *UDPManager) Broadcast(msg []byte) {
        m.mu.RLock()
        peers := make([]*udpPeer, 0, len(m.peers))
        for _, p := range m.peers {
                peers = append(peers, p)
        }
        m.mu.RUnlock()
        for _, p := range peers {
                select {
                case p.outbox <- msg:
                default:
                }
        }
}

func (m *UDPManager) BroadcastExcept(msg []byte, exceptPeerID string) {
        m.mu.RLock()
        peers := make([]*udpPeer, 0, len(m.peers))
        for _, p := range m.peers {
                peers = append(peers, p)
        }
        m.mu.RUnlock()
        for _, p := range peers {
                if p.id == exceptPeerID {
                        continue
                }
                select {
                case p.outbox <- msg:
                default:
                }
        }
}

func (m *UDPManager) emit(evt PeerEvent) {
        select {
        case m.events <- evt:
        default:
        }
}

func (m *UDPManager) Stop() {
        m.stopOnce.Do(func() {
                close(m.stopChan)
                if m.conn != nil {
                        _ = m.conn.Close()
                }
        })
        m.wg.Wait()
}
