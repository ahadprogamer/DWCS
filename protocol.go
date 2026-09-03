package protocol

import "encoding/json"

type MessageType string

const (
	MsgAcquireTask  MessageType = "acquire_task"
	MsgReleaseTask  MessageType = "release_task"
	MsgSubmitResult MessageType = "submit_result"
	MsgSubscribe    MessageType = "subscribe"
	MsgUnsubscribe  MessageType = "unsubscribe"
	MsgPing         MessageType = "ping"

	MsgTaskAvailable    MessageType = "task_available"
	MsgTaskAcquiredOK   MessageType = "task_acquired_ok"
	MsgTaskAcquiredFail MessageType = "task_acquired_fail"
	MsgResultAccepted   MessageType = "result_accepted"
	MsgResultRejected   MessageType = "result_rejected"
	MsgWorldUpdate      MessageType = "world_update"
	MsgOwnershipLost    MessageType = "ownership_lost"
	MsgPong             MessageType = "pong"
	MsgError            MessageType = "error"
)

type Envelope struct {
	Type      MessageType     `json:"type"`
	RequestID string          `json:"request_id,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

type AcquireTaskPayload struct {
	TaskID string `json:"task_id"`
}

type ReleaseTaskPayload struct {
	TaskID string `json:"task_id"`
}

type SubmitResultPayload struct {
	TaskID string          `json:"task_id"`
	Data   json.RawMessage `json:"data"`
	Meta   json.RawMessage `json:"meta,omitempty"`
}

type SubscribePayload struct {
	Tags []string `json:"tags"`
}

type TaskAvailablePayload struct {
	TaskID string          `json:"task_id"`
	Tags   []string        `json:"tags"`
	Hint   json.RawMessage `json:"hint,omitempty"`
}

type TaskAcquiredOKPayload struct {
	TaskID     string `json:"task_id"`
	LeaseUntil int64  `json:"lease_until_unix"`
}

type TaskAcquiredFailPayload struct {
	TaskID string `json:"task_id"`
	Reason string `json:"reason"`
}

type ResultAcceptedPayload struct {
	TaskID string `json:"task_id"`
}

type ResultRejectedPayload struct {
	TaskID string `json:"task_id"`
	Reason string `json:"reason"`
}

type WorldUpdatePayload struct {
	Objects []ObjectDiff `json:"objects"`
}

type ObjectDiff struct {
	ObjectID string          `json:"object_id"`
	Data     json.RawMessage `json:"data"`
	Version  uint64          `json:"version"`
	Meta     json.RawMessage `json:"meta,omitempty"`
}

type OwnershipLostPayload struct {
	TaskID string `json:"task_id"`
	Reason string `json:"reason"`
}

type ErrorPayload struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func Encode(msgType MessageType, requestID string, payload any) ([]byte, error) {
	var raw json.RawMessage
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		raw = b
	}
	return json.Marshal(Envelope{Type: msgType, RequestID: requestID, Payload: raw})
}

func EncodeEnv(msgType MessageType, requestID string, payload any) (Envelope, error) {
	var raw json.RawMessage
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return Envelope{}, err
		}
		raw = b
	}
	return Envelope{Type: msgType, RequestID: requestID, Payload: raw}, nil
}

func Decode[T any](env *Envelope) (T, error) {
	var v T
	err := json.Unmarshal(env.Payload, &v)
	return v, err
}
