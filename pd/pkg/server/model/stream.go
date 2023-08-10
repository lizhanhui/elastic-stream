package model

import (
	"github.com/pkg/errors"

	"github.com/AutoMQ/pd/api/rpcfb/rpcfb"
)

const (
	// MinStreamID is the minimum stream ID.
	MinStreamID int64 = 0
)

type CreateStreamParam struct {
	Replica           int8
	AckCount          int8
	RetentionPeriodMs int64
}

func NewCreateStreamParam(t *rpcfb.StreamT) (*CreateStreamParam, error) {
	if t == nil {
		return nil, errors.New("nil stream")
	}
	if t.Replica <= 0 {
		return nil, errors.Errorf("invalid replica %d", t.Replica)
	}
	if t.AckCount <= 0 {
		return nil, errors.Errorf("invalid ack count %d", t.AckCount)
	}

	return &CreateStreamParam{
		Replica:           t.Replica,
		AckCount:          t.AckCount,
		RetentionPeriodMs: t.RetentionPeriodMs,
	}, nil
}

type UpdateStreamParam struct {
	StreamID          int64
	Replica           int8
	AckCount          int8
	RetentionPeriodMs int64
}

func NewUpdateStreamParam(t *rpcfb.StreamT) (*UpdateStreamParam, error) {
	if t == nil {
		return nil, errors.New("nil stream")
	}
	if t.StreamId < MinStreamID {
		return nil, errors.Errorf("invalid stream id: %d < %d", t.StreamId, MinStreamID)
	}
	if t.Replica <= 0 {
		return nil, errors.Errorf("invalid replica %d", t.Replica)
	}
	if t.AckCount <= 0 {
		return nil, errors.Errorf("invalid ack count %d", t.AckCount)
	}

	return &UpdateStreamParam{
		StreamID:          t.StreamId,
		Replica:           t.Replica,
		AckCount:          t.AckCount,
		RetentionPeriodMs: t.RetentionPeriodMs,
	}, nil
}
