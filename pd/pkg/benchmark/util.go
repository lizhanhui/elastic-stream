package benchmark

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/AutoMQ/pd/api/rpcfb/rpcfb"
	sbpClient "github.com/AutoMQ/pd/pkg/sbp/client"
	"github.com/AutoMQ/pd/pkg/sbp/protocol"
	logutil "github.com/AutoMQ/pd/pkg/util/log"
)

func newClients(ctx context.Context, cnt int, logger *zap.Logger) (clients []sbpClient.Client, closeFunc func()) {
	logger = logutil.IncreaseLevel(logger, zap.InfoLevel)
	for i := 0; i < cnt; i++ {
		clients = append(clients, sbpClient.NewClient(nil, logger))
	}
	closeFunc = func() {
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		wg := sync.WaitGroup{}
		for _, client := range clients {
			wg.Add(1)
			go func(client sbpClient.Client) {
				client.Shutdown(ctx)
				wg.Done()
			}(client)
		}
		wg.Wait()
		cancel()
	}
	return
}

// heartbeatLoop mock as multiple Range Servers to send heartbeats to PD
func heartbeatLoop(ctx context.Context, rsCnt int, interval time.Duration) (closeFunc func()) {
	closeCh := make(chan struct{})

	logger := logutil.IncreaseLevel(gLogger, zap.WarnLevel).With(zap.Namespace("heartbeat-loop"))
	clients, closeClients := newClients(ctx, 1, logger)
	client := clients[0]

	heartbeats := func() {
		for i := 0; i < rsCnt; i++ {
			err := reportMetrics(ctx, client, int32(i))
			if err != nil {
				gLogger.Warn("report metrics failed", zap.Int32("range-server-ID", int32(i)), zap.Error(err))
			}
		}
	}
	// first heartbeat
	heartbeats()

	ticker := time.NewTicker(interval)
	go func() {
		gLogger.Debug("start heartbeat loop", zap.Int("range-server-count", rsCnt), zap.Duration("interval", interval))
		for {
			select {
			case <-closeCh:
				return
			case <-ticker.C:
				heartbeats()
			}
		}
	}()

	closeFunc = func() {
		close(closeCh)
		ticker.Stop()
		closeClients()
	}
	return
}

func reportMetrics(ctx context.Context, client sbpClient.Client, sID int32) error {
	req := &protocol.ReportMetricsRequest{}
	req.RangeServer = &rpcfb.RangeServerT{
		ServerId: sID,
		State:    rpcfb.RangeServerStateRANGE_SERVER_STATE_READ_WRITE,
	}
	req.Metrics = &rpcfb.RangeServerMetricsT{
		DiskFreeSpace: 1 << 30,
	}
	resp, err := client.ReportMetrics(ctx, req, pdAddr)
	if err != nil {
		return err
	}
	if resp.Status.Code != rpcfb.ErrorCodeOK {
		return errors.Errorf("report metrics failed, code: %s, msg: %s", resp.Status.Code, resp.Status.Message)
	}
	return nil
}

func createStream(ctx context.Context, client sbpClient.Client, replica int8) (sID int64, err error) {
	req := &protocol.CreateStreamRequest{}
	req.Stream = &rpcfb.StreamT{
		Replica:  replica,
		AckCount: replica,
	}
	resp, err := client.CreateStream(ctx, req, pdAddr)
	if err != nil {
		return 0, err
	}
	if resp.Status.Code != rpcfb.ErrorCodeOK {
		return 0, errors.Errorf("create stream failed, code: %s, msg: %s", resp.Status.Code, resp.Status.Message)
	}
	return resp.Stream.StreamId, nil
}

func createRange(ctx context.Context, client sbpClient.Client, sID int64, index int32, start int64) error {
	req := &protocol.CreateRangeRequest{}
	req.Range = &rpcfb.RangeT{
		StreamId: sID,
		Index:    index,
		Start:    start,
	}
	resp, err := client.CreateRange(ctx, req, pdAddr)
	if err != nil {
		return err
	}
	if resp.Status.Code != rpcfb.ErrorCodeOK {
		return errors.Errorf("create range failed, code: %s, msg: %s", resp.Status.Code, resp.Status.Message)
	}
	return nil
}

func sealRange(ctx context.Context, client sbpClient.Client, sID int64, index int32, end int64) error {
	req := &protocol.SealRangeRequest{}
	req.Range = &rpcfb.RangeT{
		StreamId: sID,
		Index:    index,
		End:      end,
	}
	req.Kind = rpcfb.SealKindPLACEMENT_DRIVER
	resp, err := client.SealRange(ctx, req, pdAddr)
	if err != nil {
		return err
	}
	if resp.Status.Code != rpcfb.ErrorCodeOK {
		return errors.Errorf("seal range failed, code: %s, msg: %s", resp.Status.Code, resp.Status.Message)
	}
	return nil
}

func commitObject(ctx context.Context, client sbpClient.Client, sID int64, rIndex int32, start, end int64, len int32, index []byte) error {
	req := &protocol.CommitObjectRequest{}
	req.Object = &rpcfb.ObjT{
		StreamId:       sID,
		RangeIndex:     rIndex,
		StartOffset:    start,
		EndOffsetDelta: int32(end - start),
		DataLen:        len,
		SparseIndex:    index,
	}
	resp, err := client.CommitObject(ctx, req, pdAddr)
	if err != nil {
		return err
	}
	if resp.Status.Code != rpcfb.ErrorCodeOK {
		return errors.Errorf("commit object failed, code: %s, msg: %s", resp.Status.Code, resp.Status.Message)
	}
	return nil
}

func newEtcdClient() (*clientv3.Client, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdURL},
		DialTimeout: 5 * time.Second,
		Logger:      logutil.IncreaseLevel(gLogger, zap.WarnLevel),
	})
	return cli, errors.WithMessage(err, "create etcd client")
}

func etcdDBSize(ctx context.Context, client clientv3.Maintenance) (size int64, sizeInUse int64, err error) {
	resp, err := client.Status(ctx, etcdURL)
	size = resp.DbSize
	sizeInUse = resp.DbSizeInUse
	err = errors.WithMessage(err, "get etcd status")
	return
}

func etcdKVInfo(ctx context.Context, client clientv3.KV) (count, revision int64, err error) {
	resp, err := client.Get(ctx, "/", clientv3.WithPrefix(), clientv3.WithCountOnly())
	count = resp.Count
	revision = resp.Header.Revision
	err = errors.WithMessage(err, "get kv count and revision")
	return
}

func etcdCompact(ctx context.Context, client clientv3.KV, rev int64) error {
	_, err := client.Compact(ctx, rev, clientv3.WithCompactPhysical())
	return errors.WithMessage(err, "compact etcd")
}

func etcdDefragment(ctx context.Context, client clientv3.Maintenance) error {
	_, err := client.Defragment(ctx, etcdURL)
	return errors.WithMessage(err, "defragment etcd")
}

func etcdReleaseSpace(ctx context.Context, client etcdInfoClient) error {
	_, rev, err := etcdKVInfo(ctx, client)
	if err != nil {
		return err
	}
	err = etcdCompact(ctx, client, rev)
	if err != nil {
		return err
	}
	err = etcdDefragment(ctx, client)
	return err
}

type etcdInfoClient interface {
	clientv3.KV
	clientv3.Maintenance
}

type etcdInfo struct {
	dbSize      int64
	dbSizeInUse int64
	kvCount     int64
	kvRevision  int64
}

// formatter of etcdInfo
func (info etcdInfo) String() string {
	return fmt.Sprintf("dbSize: %11d, dbSizeInUse: %11d, kvCount: %9d, kvRevision: %9d",
		info.dbSize, info.dbSizeInUse, info.kvCount, info.kvRevision)
}

func getEtcdInfo(ctx context.Context, client etcdInfoClient) (etcdInfo, error) {
	var info etcdInfo
	var err error
	info.dbSize, info.dbSizeInUse, err = etcdDBSize(ctx, client)
	if err != nil {
		return info, err
	}
	info.kvCount, info.kvRevision, err = etcdKVInfo(ctx, client)
	return info, err
}
