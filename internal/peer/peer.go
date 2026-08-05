package peer

import (
	"encoding/json"
	"log"

	"github.com/dwcs/backend/internal/gossip"
	"github.com/dwcs/backend/internal/merge"
	"github.com/dwcs/backend/internal/metrics"
	"github.com/dwcs/backend/internal/peering"
	"github.com/dwcs/backend/internal/protocol"
	"github.com/dwcs/backend/internal/world"
)

type Node struct {
	world   *world.World
	coord   *merge.Coordinator
	mgr     peering.PeeringManager
	gossip  *gossip.Gossip
	metrics *metrics.Recorder
	tags    []string
}

func New(w *world.World, c *merge.Coordinator, mgr peering.PeeringManager, g *gossip.Gossip, mr *metrics.Recorder, tags []string) *Node {
	return &Node{
		world:   w,
		coord:   c,
		mgr:     mgr,
		gossip:  g,
		metrics: mr,
		tags:    tags,
	}
}

func (n *Node) Run() {
	n.world.OnChange(func(evt world.ChangeEvent) {
		if evt.Kind == world.ChangeCreated || evt.Kind == world.ChangeUpdated {
			n.metrics.WorldUpdated(evt.Object.ID, evt.PrevVersion, evt.Object.Version)
			n.gossip.BroadcastExcept(protocol.MsgWorldUpdate, "", protocol.WorldUpdatePayload{
				Objects: []protocol.ObjectDiff{{
					ObjectID: evt.Object.ID,
					Data:     evt.Object.Data,
					Version:  evt.Object.Version,
					Meta:     evt.Object.Meta,
				}},
			}, "")
		}
	})

	go n.eventLoop()
}

func (n *Node) eventLoop() {
	for evt := range n.mgr.Events() {
		switch evt.Kind {
		case peering.EventPeerConnected:
			log.Printf("[peer] connected: %s", evt.PeerID)
		case peering.EventPeerClosed:
			log.Printf("[peer] closed: %s", evt.PeerID)
		case peering.EventPeerMessage:
			n.handlePeerMessage(evt.PeerID, evt.Env)
		}
	}
}

func (n *Node) handlePeerMessage(fromPeerID string, env protocol.Envelope) {
	if !n.gossip.ShouldForward(env) {
		return
	}

	switch env.Type {
	case protocol.MsgWorldUpdate:
		n.handleWorldUpdate(fromPeerID, env)
	case protocol.MsgSubmitResult:
		n.handleRemoteSubmit(fromPeerID, env)
	}
}

func (n *Node) handleWorldUpdate(fromPeerID string, env protocol.Envelope) {
	var p protocol.WorldUpdatePayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return
	}
	for _, diff := range p.Objects {
		existing, err := n.world.Get(diff.ObjectID)
		if err == nil && existing.Version >= diff.Version {
			continue
		}
		n.world.Apply(world.UpdateRequest{
			ObjectID: diff.ObjectID,
			Data:     diff.Data,
			Tags:     n.tags,
			Meta:     diff.Meta,
		})
		log.Printf("[peer] applied remote update: object=%s v=%d (from %s)", diff.ObjectID, diff.Version, fromPeerID)
		n.gossip.BroadcastExcept(protocol.MsgWorldUpdate, env.RequestID, protocol.WorldUpdatePayload{
			Objects: []protocol.ObjectDiff{diff},
		}, fromPeerID)
	}
}

func (n *Node) handleRemoteSubmit(fromPeerID string, env protocol.Envelope) {
	var p protocol.SubmitResultPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return
	}
	result := n.coord.Submit(merge.Submission{
		TaskID:  p.TaskID,
		OwnerID: fromPeerID,
		Data:    p.Data,
		Meta:    p.Meta,
	})
	if result.Accepted {
		n.metrics.TaskCompleted(p.TaskID, fromPeerID)
	}
	n.gossip.BroadcastExcept(protocol.MsgSubmitResult, env.RequestID, p, fromPeerID)
}

func (n *Node) SubmitLocalResult(taskID string, data []byte) {
	result := n.coord.Submit(merge.Submission{
		TaskID:  taskID,
		OwnerID: "local",
		Data:    data,
	})
	if result.Accepted {
		n.metrics.TaskCompleted(taskID, "local")
	}
}
