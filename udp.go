package transport

import (
        "encoding/json"
        "fmt"
        "net"
        "sync"
        "sync/atomic"
        "time"

        "github.com/dwcs/backend/internal/protocol"
        "github.com/dwcs/backend/internal/session"
)

const (
        udpClientTimeout    = 30 * time.Second
        udpClientHeartbeat  = 10 * time.Second
        udpMaxDatagram      = 65507
)

type udpClientEntry struct {
        sessionID string
        addr      *net.UDPAddr
        lastSeen  atomic.Int64
}

func newUDPClientEntry(sessionID string, addr *net.UDPAddr) *udpClientEntry {
        e := &udpClientEntry{sessionID: sessionID, addr: addr}
        e.lastSeen.Store(time.Now().UnixNano())
        return e
}

func (e *udpClientEntry) touch() {
        e.lastSeen.Store(time.Now().UnixNano())
}

func (e *udpClientEntry) isExpired() bool {
        last := time.Unix(0, e.lastSeen.Load())
        return time.Since(last) > udpClientTimeout
}

type UDPServer struct {
        addr         string
        manager      *session.Manager
        handler      Handler
        onConnect    func(sessionID string)
        onDisconnect func(sessionID string)

        conn       *net.UDPConn
        mu         sync.RWMutex
        clients    map[string]*udpClientEntry
        addrToSess map[string]*udpClientEntry

        stopOnce sync.Once
        stopChan chan struct{}
        wg       sync.WaitGroup
}

func NewUDPServer(addr string, m *session.Manager, h Handler) *UDPServer {
        return &UDPServer{
                addr:       addr,
                manager:    m,
                handler:    h,
                clients:    make(map[string]*udpClientEntry),
                addrToSess: make(map[string]*udpClientEntry),
                stopChan:   make(chan struct{}),
        }
}

func (s *UDPServer) OnConnect(fn func(sessionID string)) {
        s.onConnect = fn
}

func (s *UDPServer) OnDisconnect(fn func(sessionID string)) {
        s.onDisconnect = fn
}

func (s *UDPServer) Start() error {
        udpAddr, err := net.ResolveUDPAddr("udp", s.addr)
        if err != nil {
                return fmt.Errorf("resolve udp %s: %w", s.addr, err)
        }
        conn, err := net.ListenUDP("udp", udpAddr)
        if err != nil {
                return fmt.Errorf("listen udp %s: %w", s.addr, err)
        }
        s.conn = conn
        s.wg.Add(1)
        go s.readLoop()
        s.wg.Add(1)
        go s.writeLoop()
        s.wg.Add(1)
        go s.timeoutLoop()
        return nil
}

func (s *UDPServer) Stop() {
        s.stopOnce.Do(func() {
                close(s.stopChan)
                if s.conn != nil {
                        _ = s.conn.Close()
                }
        })
        s.wg.Wait()
}

func (s *UDPServer) readLoop() {
        defer s.wg.Done()
        buf := make([]byte, udpMaxDatagram)
        for {
                select {
                case <-s.stopChan:
                        return
                default:
                }
                _ = s.conn.SetReadDeadline(time.Now().Add(1 * time.Second))
                n, addr, err := s.conn.ReadFromUDP(buf)
                if err != nil {
                        if ne, ok := err.(net.Error); ok && ne.Timeout() {
                                continue
                        }
                        select {
                        case <-s.stopChan:
                                return
                        default:
                                continue
                        }
                }
                data := make([]byte, n)
                copy(data, buf[:n])
                s.handleDatagram(addr, data)
        }
}

func (s *UDPServer) handleDatagram(addr *net.UDPAddr, data []byte) {
        key := addr.String()
        s.mu.Lock()
        entry, exists := s.addrToSess[key]
        if !exists {
                sessionID := newSessionID()
                entry = newUDPClientEntry(sessionID, addr)
                s.clients[sessionID] = entry
                s.addrToSess[key] = entry
                sess := s.manager.Add(sessionID)
                _ = sess
                s.mu.Unlock()
                if s.onConnect != nil {
                        s.onConnect(sessionID)
                }
        } else {
                s.mu.Unlock()
        }
        entry.touch()

        var env protocol.Envelope
        if err := json.Unmarshal(data, &env); err != nil {
                s.mu.RLock()
                sess, sErr := s.manager.Get(entry.sessionID)
                s.mu.RUnlock()
                if sErr == nil {
                        errMsg, _ := protocol.Encode(protocol.MsgError, "", protocol.ErrorPayload{
                                Code:    "bad_json",
                                Message: err.Error(),
                        })
                        _ = sess.Send(errMsg)
                }
                return
        }

        if env.Type == protocol.MsgPing {
                s.mu.RLock()
                sess, sErr := s.manager.Get(entry.sessionID)
                s.mu.RUnlock()
                if sErr == nil {
                        pong, pErr := protocol.Encode(protocol.MsgPong, env.RequestID, nil)
                        if pErr == nil {
                                _ = sess.Send(pong)
                        }
                }
                return
        }

        if err := s.handler(entry.sessionID, env); err != nil {
                s.mu.RLock()
                sess, sErr := s.manager.Get(entry.sessionID)
                s.mu.RUnlock()
                if sErr == nil {
                        errMsg, _ := protocol.Encode(protocol.MsgError, env.RequestID, protocol.ErrorPayload{
                                Code:    "handler_error",
                                Message: err.Error(),
                        })
                        _ = sess.Send(errMsg)
                }
        }
}

func (s *UDPServer) writeLoop() {
        defer s.wg.Done()
        ticker := time.NewTicker(5 * time.Millisecond)
        defer ticker.Stop()
        for {
                select {
                case <-s.stopChan:
                        return
                case <-ticker.C:
                        s.flushOutboxes()
                }
        }
}

func (s *UDPServer) flushOutboxes() {
        s.mu.RLock()
        entries := make([]*udpClientEntry, 0, len(s.clients))
        for _, e := range s.clients {
                entries = append(entries, e)
        }
        s.mu.RUnlock()

        for _, e := range entries {
                sess, err := s.manager.Get(e.sessionID)
                if err != nil {
                        continue
                }
                out := sess.Outbox()
                for {
                        select {
                        case msg := <-out:
                                _ = s.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
                                _, _ = s.conn.WriteToUDP(msg, e.addr)
                        default:
                                goto next
                        }
                }
        next:
        }
}

func (s *UDPServer) timeoutLoop() {
        defer s.wg.Done()
        evictTicker := time.NewTicker(5 * time.Second)
        heartbeatTicker := time.NewTicker(udpClientHeartbeat)
        defer evictTicker.Stop()
        defer heartbeatTicker.Stop()
        for {
                select {
                case <-s.stopChan:
                        return
                case <-evictTicker.C:
                        s.evictExpired()
                case <-heartbeatTicker.C:
                        s.sendHeartbeats()
                }
        }
}

func (s *UDPServer) evictExpired() {
        s.mu.Lock()
        var evicted []*udpClientEntry
        for _, e := range s.clients {
                if e.isExpired() {
                        evicted = append(evicted, e)
                }
        }
        for _, e := range evicted {
                delete(s.clients, e.sessionID)
                delete(s.addrToSess, e.addr.String())
        }
        s.mu.Unlock()

        for _, e := range evicted {
                if s.onDisconnect != nil {
                        s.onDisconnect(e.sessionID)
                }
                s.manager.Remove(e.sessionID)
        }
}

func (s *UDPServer) sendHeartbeats() {
        ping, err := protocol.Encode(protocol.MsgPing, "", nil)
        if err != nil {
                return
        }
        s.mu.RLock()
        entries := make([]*udpClientEntry, 0, len(s.clients))
        for _, e := range s.clients {
                entries = append(entries, e)
        }
        s.mu.RUnlock()
        for _, e := range entries {
                _ = s.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
                _, _ = s.conn.WriteToUDP(ping, e.addr)
        }
}
