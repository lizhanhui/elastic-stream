package protocol

import (
	"github.com/AutoMQ/pd/api/rpcfb/rpcfb"
	"github.com/AutoMQ/pd/pkg/sbp/codec"
	fbutil "github.com/AutoMQ/pd/pkg/util/flatbuffer"
)

// response is an SBP response
type response interface{}

type InResponse interface {
	response
	unmarshaler

	// ThrottleTime returns the time in milliseconds to throttle the client.
	// It returns 0 if the response doesn't have a throttle time.
	ThrottleTime() int32
}

type noThrottleResponse struct{}

func (n noThrottleResponse) ThrottleTime() int32 {
	return 0
}

type OutResponse interface {
	response
	marshaller

	// Error sets the error status of the response.
	Error(status *rpcfb.StatusT)

	// OK sets the status of the response to rpcfb.ErrorCodeOK.
	OK()

	// IsEnd returns true if the response is the last response of a request.
	IsEnd() bool
}

// singleResponse represents a response that corresponds to a single request.
// It is used when a request is expected to have only one response.
type singleResponse struct{}

func (s singleResponse) IsEnd() bool {
	return true
}

// SystemErrorResponse is used to return the error code and error message if the system error flag of sbp is set.
type SystemErrorResponse struct {
	baseMarshaller
	baseUnmarshaler
	noThrottleResponse
	singleResponse

	rpcfb.SystemErrorT
}

func (se *SystemErrorResponse) marshalFlatBuffer() ([]byte, error) {
	return fbutil.Marshal(&se.SystemErrorT), nil
}

func (se *SystemErrorResponse) Marshal(fmt codec.Format) ([]byte, error) {
	return marshal(se, fmt)
}

func (se *SystemErrorResponse) unmarshalFlatBuffer(data []byte) error {
	se.SystemErrorT = *rpcfb.GetRootAsSystemError(data, 0).UnPack()
	return nil
}

func (se *SystemErrorResponse) Unmarshal(fmt codec.Format, data []byte) error {
	return unmarshal(se, fmt, data)
}

func (se *SystemErrorResponse) Error(status *rpcfb.StatusT) {
	se.Status = status
}

func (se *SystemErrorResponse) OK() {
	se.Status = &rpcfb.StatusT{Code: rpcfb.ErrorCodeOK}
}

// HeartbeatResponse is a response to rpcfb.OperationCodeHEARTBEAT
type HeartbeatResponse struct {
	baseMarshaller
	baseUnmarshaler
	noThrottleResponse
	singleResponse

	rpcfb.HeartbeatResponseT
}

func (hr *HeartbeatResponse) marshalFlatBuffer() ([]byte, error) {
	return fbutil.Marshal(&hr.HeartbeatResponseT), nil
}

func (hr *HeartbeatResponse) Marshal(fmt codec.Format) ([]byte, error) {
	return marshal(hr, fmt)
}

func (hr *HeartbeatResponse) unmarshalFlatBuffer(data []byte) error {
	hr.HeartbeatResponseT = *rpcfb.GetRootAsHeartbeatResponse(data, 0).UnPack()
	return nil
}

func (hr *HeartbeatResponse) Unmarshal(fmt codec.Format, data []byte) error {
	return unmarshal(hr, fmt, data)
}

func (hr *HeartbeatResponse) Error(status *rpcfb.StatusT) {
	hr.Status = status
}

func (hr *HeartbeatResponse) OK() {
	hr.Status = &rpcfb.StatusT{Code: rpcfb.ErrorCodeOK}
}

// IDAllocationResponse is a response to rpcfb.OperationCodeALLOCATE_ID
type IDAllocationResponse struct {
	baseMarshaller
	singleResponse

	rpcfb.IdAllocationResponseT
}

func (ia *IDAllocationResponse) marshalFlatBuffer() ([]byte, error) {
	return fbutil.Marshal(&ia.IdAllocationResponseT), nil
}

func (ia *IDAllocationResponse) Marshal(fmt codec.Format) ([]byte, error) {
	return marshal(ia, fmt)
}

func (ia *IDAllocationResponse) Error(status *rpcfb.StatusT) {
	ia.Status = status
}

func (ia *IDAllocationResponse) OK() {
	ia.Status = &rpcfb.StatusT{Code: rpcfb.ErrorCodeOK}
}

// ListRangeResponse is a response to rpcfb.OperationCodeLIST_RANGE
type ListRangeResponse struct {
	baseMarshaller
	singleResponse

	rpcfb.ListRangeResponseT
}

func (lr *ListRangeResponse) marshalFlatBuffer() ([]byte, error) {
	if lr.Ranges == nil {
		lr.Ranges = make([]*rpcfb.RangeT, 0)
	}
	return fbutil.Marshal(&lr.ListRangeResponseT), nil
}

func (lr *ListRangeResponse) Marshal(fmt codec.Format) ([]byte, error) {
	return marshal(lr, fmt)
}

func (lr *ListRangeResponse) Error(status *rpcfb.StatusT) {
	lr.Status = status
}

func (lr *ListRangeResponse) OK() {
	lr.Status = &rpcfb.StatusT{Code: rpcfb.ErrorCodeOK}
}

// SealRangeResponse is a response to rpcfb.OperationCodeSEAL_RANGE
type SealRangeResponse struct {
	baseMarshaller
	baseUnmarshaler
	singleResponse

	rpcfb.SealRangeResponseT
}

func (sr *SealRangeResponse) marshalFlatBuffer() ([]byte, error) {
	return fbutil.Marshal(&sr.SealRangeResponseT), nil
}

func (sr *SealRangeResponse) Marshal(fmt codec.Format) ([]byte, error) {
	return marshal(sr, fmt)
}

func (sr *SealRangeResponse) Error(status *rpcfb.StatusT) {
	sr.Status = status
}

func (sr *SealRangeResponse) OK() {
	sr.Status = &rpcfb.StatusT{Code: rpcfb.ErrorCodeOK}
}

func (sr *SealRangeResponse) unmarshalFlatBuffer(data []byte) error {
	sr.SealRangeResponseT = *rpcfb.GetRootAsSealRangeResponse(data, 0).UnPack()
	return nil
}

func (sr *SealRangeResponse) Unmarshal(fmt codec.Format, data []byte) error {
	return unmarshal(sr, fmt, data)
}

func (sr *SealRangeResponse) ThrottleTime() int32 {
	return sr.ThrottleTimeMs
}

// CreateRangeResponse is a response to rpcfb.OperationCodeCREATE_RANGE
type CreateRangeResponse struct {
	baseMarshaller
	baseUnmarshaler
	singleResponse

	rpcfb.CreateRangeResponseT
}

func (cr *CreateRangeResponse) marshalFlatBuffer() ([]byte, error) {
	return fbutil.Marshal(&cr.CreateRangeResponseT), nil
}

func (cr *CreateRangeResponse) Marshal(fmt codec.Format) ([]byte, error) {
	return marshal(cr, fmt)
}

func (cr *CreateRangeResponse) Error(status *rpcfb.StatusT) {
	cr.Status = status
}

func (cr *CreateRangeResponse) OK() {
	cr.Status = &rpcfb.StatusT{Code: rpcfb.ErrorCodeOK}
}

func (cr *CreateRangeResponse) unmarshalFlatBuffer(data []byte) error {
	cr.CreateRangeResponseT = *rpcfb.GetRootAsCreateRangeResponse(data, 0).UnPack()
	return nil
}

func (cr *CreateRangeResponse) Unmarshal(fmt codec.Format, data []byte) error {
	return unmarshal(cr, fmt, data)
}

func (cr *CreateRangeResponse) ThrottleTime() int32 {
	return cr.ThrottleTimeMs
}

// CreateStreamResponse is a response to rpcfb.OperationCodeCREATE_STREAM
type CreateStreamResponse struct {
	baseMarshaller
	baseUnmarshaler
	singleResponse

	rpcfb.CreateStreamResponseT
}

func (cs *CreateStreamResponse) marshalFlatBuffer() ([]byte, error) {
	return fbutil.Marshal(&cs.CreateStreamResponseT), nil
}

func (cs *CreateStreamResponse) Marshal(fmt codec.Format) ([]byte, error) {
	return marshal(cs, fmt)
}

func (cs *CreateStreamResponse) Error(status *rpcfb.StatusT) {
	cs.Status = status
}

func (cs *CreateStreamResponse) OK() {
	cs.Status = &rpcfb.StatusT{Code: rpcfb.ErrorCodeOK}
}

func (cs *CreateStreamResponse) unmarshalFlatBuffer(data []byte) error {
	cs.CreateStreamResponseT = *rpcfb.GetRootAsCreateStreamResponse(data, 0).UnPack()
	return nil
}

func (cs *CreateStreamResponse) Unmarshal(fmt codec.Format, data []byte) error {
	return unmarshal(cs, fmt, data)
}

func (cs *CreateStreamResponse) ThrottleTime() int32 {
	return cs.ThrottleTimeMs
}

// DeleteStreamResponse is a response to rpcfb.OperationCodeDELETE_STREAM
type DeleteStreamResponse struct {
	baseMarshaller
	singleResponse

	rpcfb.DeleteStreamResponseT
}

func (ds *DeleteStreamResponse) marshalFlatBuffer() ([]byte, error) {
	return fbutil.Marshal(&ds.DeleteStreamResponseT), nil
}

func (ds *DeleteStreamResponse) Marshal(fmt codec.Format) ([]byte, error) {
	return marshal(ds, fmt)
}

func (ds *DeleteStreamResponse) Error(status *rpcfb.StatusT) {
	ds.Status = status
}

func (ds *DeleteStreamResponse) OK() {
	ds.Status = &rpcfb.StatusT{Code: rpcfb.ErrorCodeOK}
}

// UpdateStreamResponse is a response to rpcfb.OperationCodeUPDATE_STREAM
type UpdateStreamResponse struct {
	baseMarshaller
	singleResponse

	rpcfb.UpdateStreamResponseT
}

func (us *UpdateStreamResponse) marshalFlatBuffer() ([]byte, error) {
	if us.Stream == nil {
		us.Stream = &rpcfb.StreamT{}
	}
	return fbutil.Marshal(&us.UpdateStreamResponseT), nil
}

func (us *UpdateStreamResponse) Marshal(fmt codec.Format) ([]byte, error) {
	return marshal(us, fmt)
}

func (us *UpdateStreamResponse) Error(status *rpcfb.StatusT) {
	us.Status = status
}

func (us *UpdateStreamResponse) OK() {
	us.Status = &rpcfb.StatusT{Code: rpcfb.ErrorCodeOK}
}

// DescribeStreamResponse is a response to rpcfb.OperationCodeDESCRIBE_STREAM
type DescribeStreamResponse struct {
	baseMarshaller
	singleResponse

	rpcfb.DescribeStreamResponseT
}

func (ds *DescribeStreamResponse) marshalFlatBuffer() ([]byte, error) {
	return fbutil.Marshal(&ds.DescribeStreamResponseT), nil
}

func (ds *DescribeStreamResponse) Marshal(fmt codec.Format) ([]byte, error) {
	return marshal(ds, fmt)
}

func (ds *DescribeStreamResponse) Error(status *rpcfb.StatusT) {
	ds.Status = status
}

func (ds *DescribeStreamResponse) OK() {
	ds.Status = &rpcfb.StatusT{Code: rpcfb.ErrorCodeOK}
}

// TrimStreamResponse is a response to rpcfb.OperationCodeTRIM_STREAM
type TrimStreamResponse struct {
	baseMarshaller
	singleResponse

	rpcfb.TrimStreamResponseT
}

func (ts *TrimStreamResponse) marshalFlatBuffer() ([]byte, error) {
	return fbutil.Marshal(&ts.TrimStreamResponseT), nil
}

func (ts *TrimStreamResponse) Marshal(fmt codec.Format) ([]byte, error) {
	return marshal(ts, fmt)
}

func (ts *TrimStreamResponse) Error(status *rpcfb.StatusT) {
	ts.Status = status
}

func (ts *TrimStreamResponse) OK() {
	ts.Status = &rpcfb.StatusT{Code: rpcfb.ErrorCodeOK}
}

// ReportMetricsResponse is a response to rpcfb.OperationCodeREPORT_METRICS
type ReportMetricsResponse struct {
	baseMarshaller
	baseUnmarshaler
	singleResponse
	noThrottleResponse

	rpcfb.ReportMetricsResponseT
}

func (rm *ReportMetricsResponse) marshalFlatBuffer() ([]byte, error) {
	return fbutil.Marshal(&rm.ReportMetricsResponseT), nil
}

func (rm *ReportMetricsResponse) Marshal(fmt codec.Format) ([]byte, error) {
	return marshal(rm, fmt)
}

func (rm *ReportMetricsResponse) Error(status *rpcfb.StatusT) {
	rm.Status = status
}

func (rm *ReportMetricsResponse) OK() {
	rm.Status = &rpcfb.StatusT{Code: rpcfb.ErrorCodeOK}
}

func (rm *ReportMetricsResponse) unmarshalFlatBuffer(data []byte) error {
	rm.ReportMetricsResponseT = *rpcfb.GetRootAsReportMetricsResponse(data, 0).UnPack()
	return nil
}

func (rm *ReportMetricsResponse) Unmarshal(fmt codec.Format, data []byte) error {
	return unmarshal(rm, fmt, data)
}

// DescribePDClusterResponse is a response to rpcfb.OperationCodeDESCRIBE_PLACEMENT_DRIVER
type DescribePDClusterResponse struct {
	baseMarshaller
	singleResponse

	rpcfb.DescribePlacementDriverClusterResponseT
}

func (dpd *DescribePDClusterResponse) marshalFlatBuffer() ([]byte, error) {
	if dpd.Cluster == nil {
		dpd.Cluster = &rpcfb.PlacementDriverClusterT{}
		dpd.Cluster.Nodes = make([]*rpcfb.PlacementDriverNodeT, 0)
	}
	return fbutil.Marshal(&dpd.DescribePlacementDriverClusterResponseT), nil
}

func (dpd *DescribePDClusterResponse) Marshal(fmt codec.Format) ([]byte, error) {
	return marshal(dpd, fmt)
}

func (dpd *DescribePDClusterResponse) Error(status *rpcfb.StatusT) {
	dpd.Status = status
}

func (dpd *DescribePDClusterResponse) OK() {
	dpd.Status = &rpcfb.StatusT{Code: rpcfb.ErrorCodeOK}
}

// CommitObjectResponse is a response to rpcfb.OperationCodeCOMMIT_OBJECT
type CommitObjectResponse struct {
	baseMarshaller
	baseUnmarshaler
	singleResponse

	rpcfb.CommitObjectResponseT
}

func (co *CommitObjectResponse) marshalFlatBuffer() ([]byte, error) {
	return fbutil.Marshal(&co.CommitObjectResponseT), nil
}

func (co *CommitObjectResponse) Marshal(fmt codec.Format) ([]byte, error) {
	return marshal(co, fmt)
}

func (co *CommitObjectResponse) Error(status *rpcfb.StatusT) {
	co.Status = status
}

func (co *CommitObjectResponse) OK() {
	co.Status = &rpcfb.StatusT{Code: rpcfb.ErrorCodeOK}
}

func (co *CommitObjectResponse) unmarshalFlatBuffer(data []byte) error {
	co.CommitObjectResponseT = *rpcfb.GetRootAsCommitObjectResponse(data, 0).UnPack()
	return nil
}

func (co *CommitObjectResponse) Unmarshal(fmt codec.Format, data []byte) error {
	return unmarshal(co, fmt, data)
}

func (co *CommitObjectResponse) ThrottleTime() int32 {
	return co.ThrottleTimeMs
}

// ListResourceResponse is a response to rpcfb.OperationCodeLIST_RESOURCE
type ListResourceResponse struct {
	baseMarshaller
	singleResponse

	rpcfb.ListResourceResponseT
}

func (lr *ListResourceResponse) marshalFlatBuffer() ([]byte, error) {
	if lr.Resources == nil {
		lr.Resources = make([]*rpcfb.ResourceT, 0)
	}
	return fbutil.Marshal(&lr.ListResourceResponseT), nil
}

func (lr *ListResourceResponse) Marshal(fmt codec.Format) ([]byte, error) {
	return marshal(lr, fmt)
}

func (lr *ListResourceResponse) Error(status *rpcfb.StatusT) {
	lr.Status = status
}

func (lr *ListResourceResponse) OK() {
	lr.Status = &rpcfb.StatusT{Code: rpcfb.ErrorCodeOK}
}

// WatchResourceResponse is a response to rpcfb.OperationCodeWATCH_RESOURCE
type WatchResourceResponse struct {
	baseMarshaller
	singleResponse

	rpcfb.WatchResourceResponseT
}

func (wr *WatchResourceResponse) marshalFlatBuffer() ([]byte, error) {
	if wr.Events == nil {
		wr.Events = make([]*rpcfb.ResourceEventT, 0)
	}
	return fbutil.Marshal(&wr.WatchResourceResponseT), nil
}

func (wr *WatchResourceResponse) Marshal(fmt codec.Format) ([]byte, error) {
	return marshal(wr, fmt)
}

func (wr *WatchResourceResponse) Error(status *rpcfb.StatusT) {
	wr.Status = status
}

func (wr *WatchResourceResponse) OK() {
	wr.Status = &rpcfb.StatusT{Code: rpcfb.ErrorCodeOK}
}
