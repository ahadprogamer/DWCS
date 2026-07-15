package merge

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/dwcs/backend/internal/world"
)

var ErrRejectedByMergeFunc = errors.New("result rejected by merge function")

type Submission struct {
	TaskID  string
	OwnerID string
	Data    json.RawMessage
	Meta    json.RawMessage
}

type Decision struct {
	Accept  bool
	Reason  string
	Updates []world.UpdateRequest
}

type MergeFunc func(sub Submission) Decision

type PerUpdateResult struct {
	ObjectID string
	Object   world.Object
	Error    error
}

type Result struct {
	Accepted bool
	Reason   string
	Applied  []PerUpdateResult
}

type Coordinator struct {
	world     *world.World
	mergeFunc MergeFunc
}

func New(w *world.World, fn MergeFunc) *Coordinator {
	if fn == nil {
		fn = defaultPassThrough
	}
	return &Coordinator{world: w, mergeFunc: fn}
}

func (c *Coordinator) Submit(sub Submission) Result {
	decision := c.mergeFunc(sub)

	if !decision.Accept {
		reason := decision.Reason
		if reason == "" {
			reason = "rejected by merge function"
		}
		return Result{Accepted: false, Reason: reason}
	}

	applied := make([]PerUpdateResult, 0, len(decision.Updates))
	for _, req := range decision.Updates {
		obj, err := c.world.Apply(req)
		if err != nil {
			applied = append(applied, PerUpdateResult{
				ObjectID: req.ObjectID,
				Error:    fmt.Errorf("apply failed for %q: %w", req.ObjectID, err),
			})
			continue
		}
		applied = append(applied, PerUpdateResult{
			ObjectID: req.ObjectID,
			Object:   obj,
		})
	}

	return Result{Accepted: true, Applied: applied}
}

func defaultPassThrough(sub Submission) Decision {
	return Decision{Accept: true}
}
