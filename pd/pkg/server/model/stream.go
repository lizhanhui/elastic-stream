package model

import (
	"github.com/pkg/errors"
	"go.uber.org/zap"

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

func (cs CreateStreamParam) Fields() []zap.Field {
	return []zap.Field{
		zap.Int8("create-stream-replica", cs.Replica),
		zap.Int8("create-stream-ack-count", cs.AckCount),
		zap.Int64("create-stream-retention-period-ms", cs.RetentionPeriodMs),
	}
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

func (us UpdateStreamParam) Fields() []zap.Field {
	return []zap.Field{
		zap.Int64("update-stream-id", us.StreamID),
		zap.Int8("update-stream-replica", us.Replica),
		zap.Int8("update-stream-ack-count", us.AckCount),
		zap.Int64("update-stream-retention-period-ms", us.RetentionPeriodMs),
		zap.Int64("update-stream-epoch", us.Epoch),
	}
}

type DeleteStreamParam struct {
	StreamID int64
	Epoch    int64
}

func NewDeleteStreamParam(r rpcfb.DeleteStreamRequestT) (*DeleteStreamParam, error) {
	if r.StreamId < MinStreamID {
		return nil, errors.Errorf("invalid stream id: %d < %d", r.StreamId, MinStreamID)
	}
	if r.Epoch < 0 {
		return nil, errors.Errorf("invalid epoch %d", r.Epoch)
	}

	return &DeleteStreamParam{
		StreamID: r.StreamId,
		Epoch:    r.Epoch,
	}, nil
}

func (ds DeleteStreamParam) Fields() []zap.Field {
	return []zap.Field{
		zap.Int64("delete-stream-id", ds.StreamID),
		zap.Int64("delete-stream-epoch", ds.Epoch),
	}
}

type TrimStreamParam struct {
	StreamID    int64
	StartOffset int64
	Epoch       int64
}

func NewTrimStreamParam(r rpcfb.TrimStreamRequestT) (*TrimStreamParam, error) {
	if r.StreamId < MinStreamID {
		return nil, errors.Errorf("invalid stream id: %d < %d", r.StreamId, MinStreamID)
	}
	if r.MinOffset < 0 {
		return nil, errors.Errorf("invalid min offset %d", r.MinOffset)
	}
	if r.Epoch < 0 {
		return nil, errors.Errorf("invalid epoch %d", r.Epoch)
	}

	return &TrimStreamParam{
		StreamID:    r.StreamId,
		StartOffset: r.MinOffset,
		Epoch:       r.Epoch,
	}, nil
}

func (ts TrimStreamParam) Fields() []zap.Field {
	return []zap.Field{
		zap.Int64("trim-stream-id", ts.StreamID),
		zap.Int64("trim-stream-start-offset", ts.StartOffset),
		zap.Int64("trim-stream-epoch", ts.Epoch),
	}
}
