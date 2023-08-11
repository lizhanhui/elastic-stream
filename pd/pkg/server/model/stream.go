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

func NewCreateStreamParam(s *rpcfb.StreamT) (*CreateStreamParam, error) {
	if s == nil {
		return nil, errors.New("nil stream")
	}
	if s.Replica <= 0 {
		return nil, errors.Errorf("invalid replica %d", s.Replica)
	}
	if s.AckCount <= 0 {
		return nil, errors.Errorf("invalid ack count %d", s.AckCount)
	}

	return &CreateStreamParam{
		Replica:           s.Replica,
		AckCount:          s.AckCount,
		RetentionPeriodMs: s.RetentionPeriodMs,
	}, nil
}

type UpdateStreamParam struct {
	StreamID int64

	// Any negative value means no change in following fields.
	Replica           int8
	AckCount          int8
	RetentionPeriodMs int64
	Epoch             int64
}

func NewUpdateStreamParam(s *rpcfb.StreamT) (*UpdateStreamParam, error) {
	if s == nil {
		return nil, errors.New("nil stream")
	}
	if s.StreamId < MinStreamID {
		return nil, errors.Errorf("invalid stream id: %d < %d", s.StreamId, MinStreamID)
	}
	if s.Replica == 0 {
		return nil, errors.Errorf("invalid replica %d", s.Replica)
	}
	if s.AckCount == 0 {
		return nil, errors.Errorf("invalid ack count %d", s.AckCount)
	}
	if s.StartOffset >= 0 {
		return nil, errors.Errorf("do not support update start offset %d", s.StartOffset)
	}
	if s.Replica < 0 && s.AckCount < 0 && s.RetentionPeriodMs < 0 && s.Epoch < 0 {
		return nil, errors.New("no change")
	}
	if s.Replica > 0 && s.AckCount > 0 && s.Replica < s.AckCount {
		return nil, errors.Errorf("invalid replica %d < ack count %d", s.Replica, s.AckCount)
	}

	return &UpdateStreamParam{
		StreamID:          s.StreamId,
		Replica:           s.Replica,
		AckCount:          s.AckCount,
		RetentionPeriodMs: s.RetentionPeriodMs,
		Epoch:             s.Epoch,
	}, nil
}
