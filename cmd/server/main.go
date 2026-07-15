package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dwcs/backend/internal/dispatcher"
	"github.com/dwcs/backend/internal/merge"
	"github.com/dwcs/backend/internal/metrics"
	"github.com/dwcs/backend/internal/projector"
	"github.com/dwcs/backend/internal/protocol"
	"github.com/dwcs/backend/internal/session"
	"github.com/dwcs/backend/internal/task"
	"github.com/dwcs/backend/internal/transport"
	"github.com/dwcs/backend/internal/world"
)

func main() {
	addr := flag.String("addr", ":7000", "TCP listen address")
	tick := flag.Duration("tick", 100*time.Millisecond, "world projection tick rate")
	metricsAddr := flag.String("metrics", "", "HTTP address for /metrics endpoint (empty = disabled)")
	flag.Parse()

	mr := metrics.New(*metricsAddr != "")

	w := world.New()
	r := task.NewRegistry()
	defer r.Close()
	m := session.NewManager()

	w.OnChange(func(evt world.ChangeEvent) {
		if evt.Kind == world.ChangeCreated || evt.Kind == world.ChangeUpdated {
			mr.WorldUpdated(evt.Object.ID, evt.PrevVersion, evt.Object.Version)
		}
	})

	r.OnOwnershipLost(func(evt task.LostEvent) {
		mr.TaskEvicted(evt.TaskID, evt.OwnerID, evt.Reason)
		sess, err := m.Get(evt.OwnerID)
		if err != nil {
			return
		}
		msg, _ := protocol.Encode(protocol.MsgOwnershipLost, "", protocol.OwnershipLostPayload{
			TaskID: evt.TaskID,
			Reason: evt.Reason,
		})
		_ = sess.Send(msg)
	})

	r.OnAvailable(func(evt task.AvailableEvent) {
		if evt.Reason == "registered" {
			mr.TaskRegistered(evt.TaskID, evt.Tags)
		}
		msg, err := protocol.Encode(protocol.MsgTaskAvailable, "", protocol.TaskAvailablePayload{
			TaskID: evt.TaskID,
			Tags:   evt.Tags,
			Hint:   evt.Hint,
		})
		if err != nil {
			return
		}
		m.Broadcast(evt.Tags, msg)
	})

	var mergeFunc merge.MergeFunc
	coord := merge.New(w, mergeFunc)
	if mergeFunc == nil {
		log.Printf("warning: no merge func configured — submissions will be accepted but not applied to the world")
		log.Printf("set mergeFunc in cmd/server/main.go before running in production")
	}

	disp := dispatcher.New(m, r, coord, w, mr)
	proj := projector.New(w, m, *tick, mr)
	proj.Start()
	defer proj.Stop()

	srv := transport.NewServer(*addr, m, disp.Handle)
	srv.OnConnect(func(sessionID string) {
		mr.SessionConnected(sessionID)
	})
	srv.OnDisconnect(func(sessionID string) {
		released := r.ReleaseAll(sessionID)
		mr.SessionDisconnected(sessionID, len(released))
		if len(released) > 0 {
			log.Printf("session %s disconnected: released %d tasks", sessionID, len(released))
		}
		proj.ForgetSession(sessionID)
	})
	if err := srv.Start(); err != nil {
		log.Fatalf("server: %v", err)
	}

	if *metricsAddr != "" {
		http.HandleFunc("/metrics", mr.HTTPHandler())
		go func() {
			log.Printf("metrics endpoint listening on %s", *metricsAddr)
			if err := http.ListenAndServe(*metricsAddr, nil); err != nil {
				log.Printf("metrics HTTP server error: %v", err)
			}
		}()
	}

	log.Printf("dwcs backend listening on %s (tick=%s, metrics=%s)",
		*addr, *tick, metricsAddrOrOff(*metricsAddr))

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Printf("shutting down...")
	srv.Stop()
	log.Printf("bye")
}

func metricsAddrOrOff(addr string) string {
	if addr == "" {
		return "off"
	}
	return addr
}
