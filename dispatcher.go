package dispatcher

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/dwcs/backend/internal/merge"
	"github.com/dwcs/backend/internal/metrics"
	"github.com/dwcs/backend/internal/protocol"
	"github.com/dwcs/backend/internal/session"
	"github.com/dwcs/backend/internal/task"
	"github.com/dwcs/backend/internal/world"
)

type Dispatcher struct {
	manager  *session.Manager
	registry *task.Registry
	coord    *merge.Coordinator
	world    *world.World
	metrics  *metrics.Recorder
}

func New(m *session.Manager, r *task.Registry, c *merge.Coordinator, w *world.World, mr *metrics.Recorder) *Dispatcher {
	return &Dispatcher{manager: m, registry: r, coord: c, world: w, metrics: mr}
}

func (d *Dispatcher) Handle(sessionID string, env protocol.Envelope) error {
	sess, err := d.manager.Get(sessionID)
	if err != nil {
		return err
	}

	switch env.Type {
	case protocol.MsgAcquireTask:
		return d.handleAcquire(sess, env)
	case protocol.MsgReleaseTask:
		return d.handleRelease(sess, env)
	case protocol.MsgSubmitResult:
		return d.handleSubmit(sess, env)
	case protocol.MsgSubscribe:
		return d.handleSubscribe(sess, env)
	case protocol.MsgUnsubscribe:
		return d.handleUnsubscribe(sess, env)
	case protocol.MsgPing:
		return d.handlePing(sess, env)
	default:
		return d.sendError(sess, env.RequestID, "unknown_type", fmt.Sprintf("unknown message type: %s", env.Type))
	}
}

func (d *Dispatcher) handleAcquire(sess *session.Session, env protocol.Envelope) error {
	var p protocol.AcquireTaskPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return d.sendError(sess, env.RequestID, "bad_payload", err.Error())
	}
	res, err := d.registry.Acquire(p.TaskID, sess.ID, task.LeaseOptions{})
	if err != nil {
		failPayload := protocol.TaskAcquiredFailPayload{TaskID: p.TaskID, Reason: err.Error()}
		msg, _ := protocol.Encode(protocol.MsgTaskAcquiredFail, env.RequestID, failPayload)
		return sess.Send(msg)
	}
	d.metrics.TaskClaimed(p.TaskID, sess.ID, task.DefaultLeaseDuration)
	okPayload := protocol.TaskAcquiredOKPayload{
		TaskID:     p.TaskID,
		LeaseUntil: res.LeaseUntil.Unix(),
	}
	msg, _ := protocol.Encode(protocol.MsgTaskAcquiredOK, env.RequestID, okPayload)
	return sess.Send(msg)
}

func (d *Dispatcher) handleRelease(sess *session.Session, env protocol.Envelope) error {
	var p protocol.ReleaseTaskPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return d.sendError(sess, env.RequestID, "bad_payload", err.Error())
	}
	if err := d.registry.Release(p.TaskID, sess.ID); err != nil {
		return d.sendError(sess, env.RequestID, "release_failed", err.Error())
	}
	d.metrics.TaskReleased(p.TaskID, sess.ID)
	return nil
}

func (d *Dispatcher) handleSubmit(sess *session.Session, env protocol.Envelope) error {
	var p protocol.SubmitResultPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return d.sendError(sess, env.RequestID, "bad_payload", err.Error())
	}

	t, err := d.registry.Get(p.TaskID)
	if err != nil {
		d.metrics.TaskRejected(p.TaskID, sess.ID, "task not found")
		return d.rejectResult(sess, env.RequestID, p.TaskID, "task not found")
	}
	if t.OwnerID != sess.ID {
		d.metrics.TaskRejected(p.TaskID, sess.ID, "not owner")
		return d.rejectResult(sess, env.RequestID, p.TaskID, "not owner")
	}

	result := d.coord.Submit(merge.Submission{
		TaskID:  p.TaskID,
		OwnerID: sess.ID,
		Data:    p.Data,
		Meta:    p.Meta,
	})

	if !result.Accepted {
		d.metrics.TaskRejected(p.TaskID, sess.ID, result.Reason)
		return d.rejectResult(sess, env.RequestID, p.TaskID, result.Reason)
	}

	for _, r := range result.Applied {
		if r.Error != nil {
			log.Printf("dispatcher: partial apply error task=%s object=%s: %v", p.TaskID, r.ObjectID, r.Error)
		}
	}

	d.metrics.TaskCompleted(p.TaskID, sess.ID)
	ackMsg, _ := protocol.Encode(protocol.MsgResultAccepted, env.RequestID, protocol.ResultAcceptedPayload{TaskID: p.TaskID})
	return sess.Send(ackMsg)
}

func (d *Dispatcher) handleSubscribe(sess *session.Session, env protocol.Envelope) error {
	var p protocol.SubscribePayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return d.sendError(sess, env.RequestID, "bad_payload", err.Error())
	}
	sess.Subscribe(p.Tags)

	available := d.registry.ListAvailable(p.Tags)
	for _, t := range available {
		msg, _ := protocol.Encode(protocol.MsgTaskAvailable, "", protocol.TaskAvailablePayload{
			TaskID: t.ID,
			Tags:   t.Tags,
			Hint:   t.Hint,
		})
		_ = sess.Send(msg)
	}
	return nil
}

func (d *Dispatcher) handleUnsubscribe(sess *session.Session, env protocol.Envelope) error {
	var p protocol.SubscribePayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return d.sendError(sess, env.RequestID, "bad_payload", err.Error())
	}
	sess.Unsubscribe(p.Tags)
	return nil
}

func (d *Dispatcher) handlePing(sess *session.Session, env protocol.Envelope) error {
	msg, _ := protocol.Encode(protocol.MsgPong, env.RequestID, nil)
	return sess.Send(msg)
}

func (d *Dispatcher) sendError(sess *session.Session, requestID, code, message string) error {
	msg, _ := protocol.Encode(protocol.MsgError, requestID, protocol.ErrorPayload{Code: code, Message: message})
	return sess.Send(msg)
}

func (d *Dispatcher) rejectResult(sess *session.Session, requestID, taskID, reason string) error {
	msg, _ := protocol.Encode(protocol.MsgResultRejected, requestID, protocol.ResultRejectedPayload{TaskID: taskID, Reason: reason})
	return sess.Send(msg)
}

var ErrUnknownMessageType = errors.New("unknown message type")
