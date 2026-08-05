package transport

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
        "github.com/dwcs/backend/internal/session"
)

type Handler func(sessionID string, env protocol.Envelope) error

type Server struct {
        addr         string
        manager      *session.Manager
        handler      Handler
        onConnect    func(sessionID string)
        onDisconnect func(sessionID string)
        listener     net.Listener
        wg           sync.WaitGroup
        stopOnce     sync.Once
        stopChan     chan struct{}
}

func NewServer(addr string, m *session.Manager, h Handler) *Server {
        return &Server{
                addr:     addr,
                manager:  m,
                handler:  h,
                stopChan: make(chan struct{}),
        }
}

func (s *Server) OnConnect(fn func(sessionID string)) {
        s.onConnect = fn
}

func (s *Server) OnDisconnect(fn func(sessionID string)) {
        s.onDisconnect = fn
}

func (s *Server) Start() error {
        ln, err := net.Listen("tcp", s.addr)
        if err != nil {
                return fmt.Errorf("listen %s: %w", s.addr, err)
        }
        s.listener = ln
        s.wg.Add(1)
        go s.acceptLoop()
        return nil
}

// Addr returns the address the server is listening on, or empty if not started.
func (s *Server) Addr() string {
        if s.listener == nil {
                return ""
        }
        return s.listener.Addr().String()
}

func (s *Server) Stop() {
        s.stopOnce.Do(func() {
                close(s.stopChan)
                _ = s.listener.Close()
        })
        s.wg.Wait()
}

func (s *Server) acceptLoop() {
        defer s.wg.Done()
        for {
                conn, err := s.listener.Accept()
                if err != nil {
                        select {
                        case <-s.stopChan:
                                return
                        default:
                                continue
                        }
                }
                s.wg.Add(1)
                go s.handleConn(conn)
        }
}

func (s *Server) handleConn(conn net.Conn) {
        defer s.wg.Done()
        defer conn.Close()

        sessionID := newSessionID()
        sess := s.manager.Add(sessionID)
        if s.onConnect != nil {
                s.onConnect(sessionID)
        }
        defer func() {
                if s.onDisconnect != nil {
                        s.onDisconnect(sessionID)
                }
                s.manager.Remove(sessionID)
        }()

        ctx, cancel := context.WithCancel(context.Background())
        defer cancel()

        go s.writeLoop(ctx, conn, sess)

        reader := bufio.NewReader(conn)
        for {
                select {
                case <-s.stopChan:
                        return
                default:
                }
                line, err := reader.ReadBytes('\n')
                if err != nil {
                        if errors.Is(err, io.EOF) {
                                return
                        }

                        return
                }
                var env protocol.Envelope
                if err := json.Unmarshal(line, &env); err != nil {

                        errMsg, _ := protocol.Encode(protocol.MsgError, "", protocol.ErrorPayload{
                                Code:    "bad_json",
                                Message: err.Error(),
                        })
                        _ = sess.Send(errMsg)
                        continue
                }
                if err := s.handler(sessionID, env); err != nil {
                        errMsg, _ := protocol.Encode(protocol.MsgError, "", protocol.ErrorPayload{
                                Code:    "handler_error",
                                Message: err.Error(),
                        })
                        _ = sess.Send(errMsg)
                }
        }
}

func (s *Server) writeLoop(ctx context.Context, conn net.Conn, sess *session.Session) {
        out := sess.Outbox()
        for {
                select {
                case <-ctx.Done():
                        return
                case msg, ok := <-out:
                        if !ok {
                                return
                        }
                        _ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
                        if _, err := conn.Write(msg); err != nil {
                                return
                        }
                        if _, err := conn.Write([]byte{'\n'}); err != nil {
                                return
                        }
                }
        }
}

var sessionCounter uint64
var counterMu sync.Mutex

func newSessionID() string {
        counterMu.Lock()
        defer counterMu.Unlock()
        sessionCounter++
        return fmt.Sprintf("sess-%d-%d", time.Now().UnixNano(), sessionCounter)
}
