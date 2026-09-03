package peering

import (
        "bufio"
        "context"
        "encoding/json"
        "errors"
        "fmt"
        "io"
        "net"
        "sync"
        "time"

        "github.com/dwcs/backend/internal/protocol"
)

type Peer struct {
        ID        string
        Addr      string
        conn      net.Conn
        reader    *bufio.Reader
        outbox    chan []byte
        closeOnce sync.Once
        closed    bool
        mu        sync.RWMutex
}

func newPeer(id, addr string, conn net.Conn) *Peer {
        return &Peer{
                ID:     id,
                Addr:   addr,
                conn:   conn,
                reader: bufio.NewReader(conn),
                outbox: make(chan []byte, 256),
        }
}

func (p *Peer) Send(msg []byte) error {
        p.mu.RLock()
        defer p.mu.RUnlock()
        if p.closed {
                return errors.New("peer closed")
        }
        select {
        case p.outbox <- msg:
        default:
        }
        return nil
}

func (p *Peer) Close() {
        p.closeOnce.Do(func() {
                p.mu.Lock()
                p.closed = true
                p.mu.Unlock()
                if p.conn != nil {
                        p.conn.Close()
                }
        })
}

func (p *Peer) IsClosed() bool {
        p.mu.RLock()
        defer p.mu.RUnlock()
        return p.closed
}

type PeerEvent struct {
        Kind   string
        PeerID string
        Env    protocol.Envelope
        Err    error
}

const (
        EventPeerConnected = "connected"
        EventPeerMessage   = "message"
        EventPeerClosed    = "closed"
)

type Manager struct {
        mu       sync.RWMutex
        peers    map[string]*Peer
        listener net.Listener
        events   chan PeerEvent
        stopOnce sync.Once
        stopChan chan struct{}
        wg       sync.WaitGroup
}

func NewManager() *Manager {
        return &Manager{
                peers:    make(map[string]*Peer),
                events:   make(chan PeerEvent, 1024),
                stopChan: make(chan struct{}),
        }
}

func (m *Manager) Events() <-chan PeerEvent {
        return m.events
}

func (m *Manager) Listen(addr string) error {
        ln, err := net.Listen("tcp", addr)
        if err != nil {
                return fmt.Errorf("listen %s: %w", addr, err)
        }
        m.listener = ln
        m.wg.Add(1)
        go m.acceptLoop()
        return nil
}

// Addr returns the address the manager is listening on, or empty if not started.
func (m *Manager) Addr() string {
        m.mu.RLock()
        defer m.mu.RUnlock()
        if m.listener == nil {
                return ""
        }
        return m.listener.Addr().String()
}

func (m *Manager) acceptLoop() {
        defer m.wg.Done()
        for {
                conn, err := m.listener.Accept()
                if err != nil {
                        select {
                        case <-m.stopChan:
                                return
                        default:
                                continue
                        }
                }
                m.wg.Add(1)
                go m.handleInbound(conn)
        }
}

func (m *Manager) handleInbound(conn net.Conn) {
        defer m.wg.Done()
        peerID := fmt.Sprintf("peer-%d-%d", time.Now().UnixNano(), len(m.peers))
        p := newPeer(peerID, conn.RemoteAddr().String(), conn)
        m.addPeer(p)
        m.emit(PeerEvent{Kind: EventPeerConnected, PeerID: peerID})
        defer func() {
                m.removePeer(peerID)
                m.emit(PeerEvent{Kind: EventPeerClosed, PeerID: peerID})
        }()
        ctx, cancel := context.WithCancel(context.Background())
        defer cancel()
        go m.writeLoop(ctx, p)
        for {
                line, err := p.reader.ReadBytes('\n')
                if err != nil {
                        if errors.Is(err, io.EOF) {
                                return
                        }
                        return
                }
                var env protocol.Envelope
                if err := json.Unmarshal(line, &env); err != nil {
                        continue
                }
                m.emit(PeerEvent{Kind: EventPeerMessage, PeerID: peerID, Env: env})
        }
}

func (m *Manager) Dial(addr string) error {
        conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
        if err != nil {
                return fmt.Errorf("dial %s: %w", addr, err)
        }
        peerID := fmt.Sprintf("peer-%d-%d", time.Now().UnixNano(), len(m.peers))
        p := newPeer(peerID, addr, conn)
        m.addPeer(p)
        m.emit(PeerEvent{Kind: EventPeerConnected, PeerID: peerID})
        m.wg.Add(1)
        go m.handleOutbound(p)
        return nil
}

func (m *Manager) handleOutbound(p *Peer) {
        defer m.wg.Done()
        defer func() {
                m.removePeer(p.ID)
                m.emit(PeerEvent{Kind: EventPeerClosed, PeerID: p.ID})
        }()
        ctx, cancel := context.WithCancel(context.Background())
        defer cancel()
        go m.writeLoop(ctx, p)
        for {
                line, err := p.reader.ReadBytes('\n')
                if err != nil {
                        return
                }
                var env protocol.Envelope
                if err := json.Unmarshal(line, &env); err != nil {
                        continue
                }
                m.emit(PeerEvent{Kind: EventPeerMessage, PeerID: p.ID, Env: env})
        }
}

func (m *Manager) writeLoop(ctx context.Context, p *Peer) {
        for {
                select {
                case <-ctx.Done():
                        return
                case msg, ok := <-p.outbox:
                        if !ok {
                                return
                        }
                        _ = p.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
                        if _, err := p.conn.Write(msg); err != nil {
                                return
                        }
                        if _, err := p.conn.Write([]byte{'\n'}); err != nil {
                                return
                        }
                }
        }
}

func (m *Manager) addPeer(p *Peer) {
        m.mu.Lock()
        m.peers[p.ID] = p
        m.mu.Unlock()
}

func (m *Manager) removePeer(id string) {
        m.mu.Lock()
        p, ok := m.peers[id]
        if ok {
                delete(m.peers, id)
        }
        m.mu.Unlock()
        if ok {
                p.Close()
        }
}

func (m *Manager) Broadcast(msg []byte) {
        m.mu.RLock()
        snap := make([]*Peer, 0, len(m.peers))
        for _, p := range m.peers {
                snap = append(snap, p)
        }
        m.mu.RUnlock()
        for _, p := range snap {
                _ = p.Send(msg)
        }
}

func (m *Manager) BroadcastExcept(msg []byte, exceptPeerID string) {
        m.mu.RLock()
        snap := make([]*Peer, 0, len(m.peers))
        for _, p := range m.peers {
                snap = append(snap, p)
        }
        m.mu.RUnlock()
        for _, p := range snap {
                if p.ID == exceptPeerID {
                        continue
                }
                _ = p.Send(msg)
        }
}

func (m *Manager) PeerCount() int {
        m.mu.RLock()
        defer m.mu.RUnlock()
        return len(m.peers)
}

func (m *Manager) emit(evt PeerEvent) {
        select {
        case m.events <- evt:
        default:
        }
}

func (m *Manager) Stop() {
        m.stopOnce.Do(func() {
                close(m.stopChan)
                if m.listener != nil {
                        m.listener.Close()
                }
        })
        m.mu.Lock()
        for _, p := range m.peers {
                p.Close()
        }
        m.mu.Unlock()
        m.wg.Wait()
}
