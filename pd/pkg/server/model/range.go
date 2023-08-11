package model

import (
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/AutoMQ/pd/api/rpcfb/rpcfb"
)

const (
	// MinRangeIndex is the minimum range index.
	MinRangeIndex int32 = 0
)

type SealRangeParam struct {
	StreamID int64
	Epoch    int64
	Index    int32
	End      int64
}

func NewSealRangeParam(r *rpcfb.RangeT) (*SealRangeParam, error) {
	if r == nil {
		return nil, errors.New("nil range")
	}
	if r.StreamId < MinStreamID {
		return nil, errors.Errorf("invalid stream id: %d < %d", r.StreamId, MinStreamID)
	}
	if r.Index < MinRangeIndex {
		return nil, errors.Errorf("invalid index %d < %d", r.Index, MinRangeIndex)
	}
	if r.Epoch < 0 {
		return nil, errors.Errorf("invalid epoch %d", r.Epoch)
	}
	if r.End < 0 {
		return nil, errors.Errorf("invalid end %d", r.End)
	}

	return &SealRangeParam{
		StreamID: r.StreamId,
		Epoch:    r.Epoch,
		Index:    r.Index,
		End:      r.End,
	}, nil
}

func (sr SealRangeParam) Fields() []zap.Field {
	return []zap.Field{
		zap.Int64("seal-range-stream-id", sr.StreamID),
		zap.Int64("seal-range-epoch", sr.Epoch),
		zap.Int32("seal-range-index", sr.Index),
		zap.Int64("seal-range-end", sr.End),
	}
}

type CreateRangeParam struct {
	StreamID int64
	Epoch    int64
	Index    int32
	Start    int64
}

func NewCreateRangeParam(r *rpcfb.RangeT) (*CreateRangeParam, error) {
	if r == nil {
		return nil, errors.New("nil range")
	}
	if r.StreamId < MinStreamID {
		return nil, errors.Errorf("invalid stream id: %d < %d", r.StreamId, MinStreamID)
	}
	if r.Index < MinRangeIndex {
		return nil, errors.Errorf("invalid index %d < %d", r.Index, MinRangeIndex)
	}
	if r.Epoch < 0 {
		return nil, errors.Errorf("invalid epoch %d", r.Epoch)
	}
	if r.Start < 0 {
		return nil, errors.Errorf("invalid start %d", r.Start)
	}

	return &CreateRangeParam{
		StreamID: r.StreamId,
		Epoch:    r.Epoch,
		Index:    r.Index,
		Start:    r.Start,
	}, nil
}

func (cr CreateRangeParam) Fields() []zap.Field {
	return []zap.Field{
		zap.Int64("create-range-stream-id", cr.StreamID),
		zap.Int64("create-range-epoch", cr.Epoch),
		zap.Int32("create-range-index", cr.Index),
		zap.Int64("create-range-start", cr.Start),
	}
}
