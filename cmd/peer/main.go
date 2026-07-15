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
	listen := flag.String("listen", ":7000", "TCP address to listen for inbound peer connections")
	peers := flag.String("peers", "", "comma-separated list of peer addresses to dial")
	tags := flag.String("tags", "*", "comma-separated tags this peer subscribes to")
	metricsAddr := flag.String("metrics", "", "HTTP address for /metrics endpoint (empty = disabled)")
	workInterval := flag.Duration("work", 0, "if set, periodically submit a fake result for testing (e.g. 500ms)")
	name := flag.String("name", "", "peer display name for logs")
	flag.Parse()

	if *name == "" {
		*name = fmt.Sprintf("p%d", rand.Intn(1000))
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

	mgr := peering.NewManager()
	g := gossip.New(mgr)
	g.Start()
	defer g.Stop()

	tagList := parseTags(*tags)
	node := peer.New(w, coord, mgr, g, mr, tagList)
	node.Run()

	if err := mgr.Listen(*listen); err != nil {
		log.Fatalf("peer: %v", err)
	}
	log.Printf("[%s] p2p peer listening on %s (tags=%v)", *name, *listen, tagList)

	if *peers != "" {
		for _, addr := range strings.Split(*peers, ",") {
			addr = strings.TrimSpace(addr)
			if addr == "" {
				continue
			}
			go func(a string) {
				time.Sleep(500 * time.Millisecond)
				log.Printf("[%s] dialing peer %s", *name, a)
				if err := mgr.Dial(a); err != nil {
					log.Printf("[%s] dial %s failed: %v", *name, a, err)
				}
			}(addr)
		}
	}

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
				mgr.Broadcast(msg)
				node.SubmitLocalResult(objID, data)
				log.Printf("[%s] submitted %s = {x:%d, y:%d}", *name, objID, x, y)
			}
		}()
	}

	log.Printf("[%s] dwcs p2p peer started (peers=%d)", *name, mgr.PeerCount())

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Printf("[%s] shutting down...", *name)
	mgr.Stop()
	log.Printf("[%s] bye", *name)
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
