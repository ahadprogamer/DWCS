package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/dwcs/backend/internal/gossip"
	"github.com/dwcs/backend/internal/merge"
	"github.com/dwcs/backend/internal/metrics"
	"github.com/dwcs/backend/internal/peer"
	"github.com/dwcs/backend/internal/peering"
	"github.com/dwcs/backend/internal/protocol"
	"github.com/dwcs/backend/internal/world"
)

func main() {
	listen := flag.String("listen", ":7000", "address to listen for inbound peer connections")
	peers := flag.String("peers", "", "comma-separated list of peer addresses to dial")
	tags := flag.String("tags", "*", "comma-separated tags this peer subscribes to")
	metricsAddr := flag.String("metrics", "", "HTTP address for /metrics endpoint (empty = disabled)")
	workInterval := flag.Duration("work", 0, "if set, periodically submit a fake result for testing (e.g. 500ms)")
	name := flag.String("name", "", "peer display name for logs")
	transportKind := flag.String("transport", "tcp", "transport to use: tcp, udp, or both")
	flag.Parse()

	if *name == "" {
		*name = fmt.Sprintf("p%d", rand.Intn(1000))
	}

	switch *transportKind {
	case "tcp", "udp", "both":
	default:
		log.Fatalf("invalid -transport %q: must be tcp, udp, or both", *transportKind)
	}

	mr := metrics.New(*metricsAddr != "")
	w := world.New()

	mergeFunc := func(sub merge.Submission) merge.Decision {
		var p map[string]int
		if err := json.Unmarshal(sub.Data, &p); err != nil {
			return merge.Decision{Accept: false, Reason: err.Error()}
		}
		data, _ := json.Marshal(p)
		return merge.Decision{
			Accept: true,
			Updates: []world.UpdateRequest{{
				ObjectID: sub.TaskID,
				Data:     data,
				Tags:     []string{"test"},
			}},
		}
	}
	coord := merge.New(w, mergeFunc)

	tagList := parseTags(*tags)
	peerAddrs := parsePeerAddrs(*peers)

	var activeMgr peering.PeeringManager

	switch *transportKind {
	case "tcp":
		mgr := peering.NewManager()
		if err := mgr.Listen(*listen); err != nil {
			log.Fatalf("peer tcp listen: %v", err)
		}
		log.Printf("[%s] tcp peer listening on %s", *name, *listen)
		for _, addr := range peerAddrs {
			dialPeer(*name, addr, mgr)
		}
		activeMgr = mgr

	case "udp":
		mgr := peering.NewUDPManager()
		if err := mgr.Listen(*listen); err != nil {
			log.Fatalf("peer udp listen: %v", err)
		}
		log.Printf("[%s] udp peer listening on %s", *name, *listen)
		for _, addr := range peerAddrs {
			dialPeer(*name, addr, mgr)
		}
		activeMgr = mgr

	case "both":
		tcpMgr := peering.NewManager()
		if err := tcpMgr.Listen(*listen); err != nil {
			log.Fatalf("peer tcp listen: %v", err)
		}
		log.Printf("[%s] tcp peer listening on %s", *name, *listen)

		udpMgr := peering.NewUDPManager()
		if err := udpMgr.Listen(*listen); err != nil {
			log.Fatalf("peer udp listen: %v", err)
		}
		log.Printf("[%s] udp peer listening on %s", *name, *listen)

		for _, addr := range peerAddrs {
			dialPeer(*name, addr, tcpMgr)
			dialPeer(*name, addr, udpMgr)
		}
		activeMgr = peering.NewFanInManager(tcpMgr, udpMgr)
	}

	g := gossip.New(activeMgr)
	g.Start()
	defer g.Stop()

	node := peer.New(w, coord, activeMgr, g, mr, tagList)
	node.Run()

	if *metricsAddr != "" {
		http.HandleFunc("/metrics", mr.HTTPHandler())
		go func() {
			log.Printf("[%s] metrics endpoint listening on %s", *name, *metricsAddr)
			if err := http.ListenAndServe(*metricsAddr, nil); err != nil {
				log.Printf("[%s] metrics HTTP server error: %v", *name, err)
			}
		}()
	}

	if *workInterval > 0 {
		go func() {
			ticker := time.NewTicker(*workInterval)
			defer ticker.Stop()
			objID := fmt.Sprintf("obj-%s", *name)
			for range ticker.C {
				x := rand.Intn(100)
				y := rand.Intn(100)
				data, _ := json.Marshal(map[string]int{"x": x, "y": y})
				msg, _ := protocol.Encode(protocol.MsgSubmitResult, "", protocol.SubmitResultPayload{
					TaskID: objID,
					Data:   data,
				})
				activeMgr.Broadcast(msg)
				node.SubmitLocalResult(objID, data)
				log.Printf("[%s] submitted %s = {x:%d, y:%d}", *name, objID, x, y)
			}
		}()
	}

	log.Printf("[%s] dwcs p2p peer started (transport=%s, peers=%d)", *name, *transportKind, activeMgr.PeerCount())

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Printf("[%s] shutting down...", *name)
	activeMgr.Stop()
	log.Printf("[%s] bye", *name)
}

func dialPeer(name, addr string, mgr peering.PeeringManager) {
	go func() {
		time.Sleep(500 * time.Millisecond)
		log.Printf("[%s] dialing peer %s", name, addr)
		if err := mgr.Dial(addr); err != nil {
			log.Printf("[%s] dial %s failed: %v", name, addr, err)
		}
	}()
}

func parseTags(s string) []string {
	if s == "" || s == "*" {
		return []string{"*"}
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func parsePeerAddrs(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
