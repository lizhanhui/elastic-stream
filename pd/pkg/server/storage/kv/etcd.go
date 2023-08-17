// Copyright 2016 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import (
	"bytes"
	"context"
	"fmt"

	"github.com/pkg/errors"
	etcdrpc "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"

	"github.com/AutoMQ/pd/pkg/server/model"
	"github.com/AutoMQ/pd/pkg/util/etcdutil"
	"github.com/AutoMQ/pd/pkg/util/traceutil"
)

const (
	// A buffer in order to reduce times of context switch.
	_watchChanCap = 128

	// The max retry count when txn commit failed with model.ErrKVDataModified.
	_txnMaxRetry = 3
)

// Etcd is a kv based on etcd.
type Etcd struct {
	rootPath   []byte
	newTxnFunc func(ctx context.Context) clientv3.Txn // WARNING: do not call `If` on the returned txn.
	maxTxnOps  uint

	watcher clientv3.Watcher
	lease   clientv3.Lease

	lg *zap.Logger
}

type Client interface {
	clientv3.KV
	clientv3.Lease
	clientv3.Watcher
}

// EtcdParam is used to create a new etcd kv.
type EtcdParam struct {
	Client Client
	// rootPath is the prefix of all keys in etcd.
	RootPath string
	// cmpFunc is used to create a transaction. If cmpFunc is nil, the transaction will not have any condition.
	CmpFunc func() clientv3.Cmp
	// maxTxnOps is the max number of operations in a transaction. It is an etcd server configuration.
	// If maxTxnOps is 0, it will use the default value (128).
	MaxTxnOps uint
}

// NewEtcd creates a new etcd kv.
func NewEtcd(param EtcdParam, lg *zap.Logger) *Etcd {
	logger := lg.With(zap.String("root-path", param.RootPath))
	e := &Etcd{
		rootPath:  []byte(param.RootPath),
		maxTxnOps: param.MaxTxnOps,
		watcher:   param.Client,
		lease:     param.Client,
		lg:        logger,
	}

	if e.maxTxnOps == 0 {
		e.maxTxnOps = embed.DefaultMaxTxnOps
	}

	if param.CmpFunc != nil {
		e.newTxnFunc = func(ctx context.Context) clientv3.Txn {
			// cmpFunc should be evaluated lazily.
			return etcdutil.NewTxn(ctx, param.Client, logger.With(traceutil.TraceLogField(ctx))).If(param.CmpFunc())
		}
	} else {
		e.newTxnFunc = func(ctx context.Context) clientv3.Txn {
			return etcdutil.NewTxn(ctx, param.Client, logger.With(traceutil.TraceLogField(ctx)))
		}
	}

	return e
}

// Get returns model.ErrKVTxnFailed if EtcdParam.CmpFunc evaluates to false.
func (e *Etcd) Get(ctx context.Context, k []byte) ([]byte, error) {
	if len(k) == 0 {
		return nil, nil
	}

	kvs, err := e.BatchGet(ctx, [][]byte{k}, false)
	if err != nil {
		return nil, errors.WithMessage(err, "kv get")
	}

	for _, kv := range kvs {
		if bytes.Equal(kv.Key, k) {
			return kv.Value, nil
		}
	}
	return nil, nil
}

// BatchGet returns model.ErrKVTxnFailed if EtcdParam.CmpFunc evaluates to false.
// If inTxn is true, BatchGet returns model.ErrKVTooManyTxnOps if the number of keys exceeds EtcdParam.MaxTxnOps.
func (e *Etcd) BatchGet(ctx context.Context, keys [][]byte, inTxn bool) ([]KeyValue, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	batchSize := int(e.maxTxnOps)
	if inTxn && len(keys) > batchSize {
		return nil, errors.WithMessage(model.ErrKVTooManyTxnOps, "kv batch get")
	}

	kvs := make([]KeyValue, 0, len(keys))

	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}
		batchKeys := keys[i:end]

		ops := make([]clientv3.Op, 0, len(batchKeys))
		for _, k := range batchKeys {
			if len(k) == 0 {
				continue
			}
			key := e.addPrefix(k)
			ops = append(ops, clientv3.OpGet(string(key)))
		}

		txn := e.newTxnFunc(ctx).Then(ops...)
		resp, err := txn.Commit()
		if err != nil {
			return nil, errors.WithMessage(err, "kv batch get")
		}
		if !resp.Succeeded {
			return nil, errors.WithMessage(model.ErrKVTxnFailed, "kv batch get")
		}

		for _, resp := range resp.Responses {
			rangeResp := resp.GetResponseRange()
			if rangeResp == nil {
				continue
			}
			for _, kv := range rangeResp.Kvs {
				if !e.hasPrefix(kv.Key) {
					continue
				}
				kvs = append(kvs, KeyValue{
					Key:   e.trimPrefix(kv.Key),
					Value: kv.Value,
				})
			}
		}
	}

	return kvs, nil
}

// GetByRange returns model.ErrKVTxnFailed if EtcdParam.CmpFunc evaluates to false.
// It returns model.ErrKVCompacted if the requested revision has been compacted.
func (e *Etcd) GetByRange(ctx context.Context, r Range, rev int64, limit int64, desc bool) ([]KeyValue, int64, bool, error) {
	if len(r.StartKey) == 0 || len(r.EndKey) == 0 {
		return nil, 0, false, nil
	}

	startKey := e.addPrefix(r.StartKey)
	endKey := e.addPrefix(r.EndKey)

	opts := []clientv3.OpOption{clientv3.WithRange(string(endKey))}
	if rev > 0 {
		opts = append(opts, clientv3.WithRev(rev))
	}
	if limit > 0 {
		opts = append(opts, clientv3.WithLimit(limit))
	}
	if desc {
		opts = append(opts, clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
	}

	resp, err := e.newTxnFunc(ctx).Then(clientv3.OpGet(string(startKey), opts...)).Commit()
	if err != nil {
		if err == etcdrpc.ErrCompacted {
			return nil, 0, false, errors.WithMessagef(model.ErrKVCompacted, "kv get by range, revision %d", rev)
		}
		return nil, 0, false, errors.WithMessage(err, "kv get by range")
	}
	if !resp.Succeeded {
		return nil, 0, false, errors.WithMessage(model.ErrKVTxnFailed, "kv get by range")
	}

	// When the transaction succeeds, the number of responses is always 1 and is always a range response.
	rangeResp := resp.Responses[0].GetResponseRange()

	kvs := make([]KeyValue, 0, len(rangeResp.Kvs))
	for _, kv := range rangeResp.Kvs {
		if !e.hasPrefix(kv.Key) {
			continue
		}
		kvs = append(kvs, KeyValue{
			Key:   e.trimPrefix(kv.Key),
			Value: kv.Value,
		})
	}

	returnedRV := rev
	if returnedRV <= 0 {
		returnedRV = rangeResp.Header.Revision
	}

	return kvs, returnedRV, rangeResp.More, nil
}

func (e *Etcd) Watch(ctx context.Context, prefix []byte, rev int64, filter Filter) Watcher {
	ctx = clientv3.WithRequireLeader(ctx)
	key := e.addPrefix(prefix)
	opts := []clientv3.OpOption{clientv3.WithPrevKV(), clientv3.WithPrefix()}
	if rev > 0 {
		opts = append(opts, clientv3.WithRev(rev))
	}

	wch := e.watcher.Watch(ctx, string(key), opts...)
	logger := e.lg.With(zap.ByteString("prefix", prefix), zap.Int64("revision", rev), traceutil.TraceLogField(ctx))
	watcher := e.newWatchChan(ctx, wch, filter, logger)
	go watcher.run()

	return watcher
}

// Put returns model.ErrKVTxnFailed if EtcdParam.CmpFunc evaluates to false.
func (e *Etcd) Put(ctx context.Context, k, v []byte, prevKV bool, ttl int64) ([]byte, error) {
	if len(k) == 0 {
		return nil, nil
	}

	prevKVs, err := e.BatchPut(ctx, []KeyValue{{Key: k, Value: v}}, prevKV, false, ttl)
	if err != nil {
		return nil, errors.WithMessage(err, "kv put")
	}

	if !prevKV {
		return nil, nil
	}

	for _, kv := range prevKVs {
		if bytes.Equal(kv.Key, k) {
			return kv.Value, nil
		}
	}
	return nil, nil
}

// BatchPut returns model.ErrKVTxnFailed if EtcdParam.CmpFunc evaluates to false.
// If inTxn is true, BatchPut returns model.ErrKVTooManyTxnOps if the number of kvs exceeds EtcdParam.MaxTxnOps.
func (e *Etcd) BatchPut(ctx context.Context, kvs []KeyValue, prevKV bool, inTxn bool, ttl int64) ([]KeyValue, error) {
	if len(kvs) == 0 {
		return nil, nil
	}
	batchSize := int(e.maxTxnOps)
	if inTxn && len(kvs) > batchSize {
		return nil, errors.WithMessage(model.ErrKVTooManyTxnOps, "kv batch put")
	}

	var prevKVs []KeyValue
	if prevKV {
		prevKVs = make([]KeyValue, 0, len(kvs))
	}

	var opts []clientv3.OpOption
	if prevKV {
		opts = append(opts, clientv3.WithPrevKV())
	}
	if ttl > 0 {
		leaseID, err := e.grantLease(ctx, ttl)
		if err != nil {
			return nil, err
		}
		opts = append(opts, clientv3.WithLease(leaseID))
	}

	for i := 0; i < len(kvs); i += batchSize {
		end := i + batchSize
		if end > len(kvs) {
			end = len(kvs)
		}
		batchKVs := kvs[i:end]

		ops := make([]clientv3.Op, 0, len(batchKVs))
		for _, kv := range batchKVs {
			if len(kv.Key) == 0 {
				continue
			}
			key := e.addPrefix(kv.Key)
			ops = append(ops, clientv3.OpPut(string(key), string(kv.Value), opts...))
		}

		// TODO: skip empty ops
		txn := e.newTxnFunc(ctx).Then(ops...)
		resp, err := txn.Commit()
		if err != nil {
			return nil, errors.WithMessage(err, "kv batch put")
		}
		if !resp.Succeeded {
			return nil, errors.WithMessage(model.ErrKVTxnFailed, "kv batch put")
		}

		if !prevKV {
			continue
		}
		for _, resp := range resp.Responses {
			putResp := resp.GetResponsePut()
			if putResp.PrevKv == nil {
				continue
			}
			if !e.hasPrefix(putResp.PrevKv.Key) {
				continue
			}
			prevKVs = append(prevKVs, KeyValue{
				Key:   e.trimPrefix(putResp.PrevKv.Key),
				Value: putResp.PrevKv.Value,
			})
		}
	}

	return prevKVs, nil
}

// Delete returns model.ErrKVTxnFailed if EtcdParam.CmpFunc evaluates to false.
func (e *Etcd) Delete(ctx context.Context, k []byte, prevKV bool) ([]byte, error) {
	if len(k) == 0 {
		return nil, nil
	}

	prevKVs, err := e.BatchDelete(ctx, [][]byte{k}, prevKV, false)
	if err != nil {
		return nil, errors.WithMessage(err, "kv delete")
	}

	if !prevKV {
		return nil, nil
	}
	for _, kv := range prevKVs {
		if bytes.Equal(kv.Key, k) {
			return kv.Value, nil
		}
	}
	return nil, nil
}

// BatchDelete returns model.ErrKVTxnFailed if EtcdParam.CmpFunc evaluates to false.
// If inTxn is true, BatchDelete returns model.ErrKVTooManyTxnOps if the number of keys exceeds EtcdParam.MaxTxnOps.
func (e *Etcd) BatchDelete(ctx context.Context, keys [][]byte, prevKV bool, inTxn bool) ([]KeyValue, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	batchSize := int(e.maxTxnOps)
	if inTxn && len(keys) > batchSize {
		return nil, errors.WithMessage(model.ErrKVTooManyTxnOps, "kv batch delete")
	}

	var prevKVs []KeyValue
	if prevKV {
		prevKVs = make([]KeyValue, 0, len(keys))
	}

	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}
		batchKeys := keys[i:end]

		ops := make([]clientv3.Op, 0, len(batchKeys))
		var opts []clientv3.OpOption
		if prevKV {
			opts = append(opts, clientv3.WithPrevKV())
		}
		for _, k := range batchKeys {
			if len(k) == 0 {
				continue
			}
			key := e.addPrefix(k)
			ops = append(ops, clientv3.OpDelete(string(key), opts...))
		}

		txn := e.newTxnFunc(ctx).Then(ops...)
		resp, err := txn.Commit()
		if err != nil {
			return nil, errors.WithMessage(err, "kv batch delete")
		}
		if !resp.Succeeded {
			return nil, errors.WithMessage(model.ErrKVTxnFailed, "kv batch delete")
		}

		if !prevKV {
			continue
		}
		for _, resp := range resp.Responses {
			deleteResp := resp.GetResponseDeleteRange()
			for _, kv := range deleteResp.PrevKvs {
				if !e.hasPrefix(kv.Key) {
					continue
				}
				prevKVs = append(prevKVs, KeyValue{
					Key:   e.trimPrefix(kv.Key),
					Value: kv.Value,
				})
			}
		}
	}

	return prevKVs, nil
}

// DeleteByRange returns model.ErrKVTxnFailed if EtcdParam.CmpFunc evaluates to false.
func (e *Etcd) DeleteByRange(ctx context.Context, r Range) (int, error) {
	if len(r.StartKey) == 0 && len(r.EndKey) == 0 {
		return 0, nil
	}

	startKey := e.addPrefix(r.StartKey)
	endKey := e.addPrefix(r.EndKey)
	opts := []clientv3.OpOption{clientv3.WithRange(string(endKey))}

	resp, err := e.newTxnFunc(ctx).Then(clientv3.OpDelete(string(startKey), opts...)).Commit()
	if err != nil {
		return 0, errors.WithMessage(err, "kv delete by range")
	}
	if !resp.Succeeded {
		return 0, errors.WithMessage(model.ErrKVTxnFailed, "kv delete by range")
	}

	// When the transaction succeeds, the number of responses is always 1 and is always a DeleteRangeResponse.
	delResp := resp.Responses[0].GetResponseDeleteRange()

	return int(delResp.Deleted), nil
}

// ExecInTxn returns model.ErrKVTxnFailed if EtcdParam.CmpFunc evaluates to false.
// It returns model.ErrKVDataModified if any key is modified by others.
func (e *Etcd) ExecInTxn(ctx context.Context, f func(kv BasicKV) error) (err error) {
	// retry if txn failed
	for i := 0; i < _txnMaxRetry; i++ {
		txn := e.newEtcdTxn(ctx)
		err = f(txn)
		if err != nil {
			return
		}

		err = txn.Commit()
		if err == nil {
			return
		}
		if !errors.Is(err, model.ErrKVDataModified) {
			return
		}
	}
	return
}

func (e *Etcd) GetPrefixRangeEnd(p []byte) []byte {
	prefix := e.addPrefix(p)
	end := []byte(clientv3.GetPrefixRangeEnd(string(prefix)))
	return e.trimPrefix(end)
}

func (e *Etcd) Logger() *zap.Logger {
	return e.lg
}

type prefixHandler interface {
	addPrefix(k []byte) []byte
	trimPrefix(k []byte) []byte
	hasPrefix(k []byte) bool
}

func (e *Etcd) addPrefix(k []byte) []byte {
	return bytes.Join([][]byte{e.rootPath, k}, []byte(KeySeparator))
}

func (e *Etcd) trimPrefix(k []byte) []byte {
	return k[len(e.rootPath)+len(KeySeparator):]
}

func (e *Etcd) hasPrefix(k []byte) bool {
	return len(k) >= len(e.rootPath)+len(KeySeparator) &&
		bytes.Equal(k[:len(e.rootPath)], e.rootPath) &&
		string(k[len(e.rootPath):len(e.rootPath)+len(KeySeparator)]) == KeySeparator
}

type lease interface {
	grantLease(ctx context.Context, ttl int64) (clientv3.LeaseID, error)
}

func (e *Etcd) grantLease(ctx context.Context, ttl int64) (clientv3.LeaseID, error) {
	resp, err := e.lease.Grant(ctx, ttl)
	if err != nil {
		return 0, errors.WithMessagef(err, "grant lease with ttl %d", ttl)
	}
	return resp.ID, nil
}

type watchChan struct {
	prefixHandler

	ctx    context.Context
	cancel context.CancelFunc

	ch         clientv3.WatchChan
	filter     Filter
	resultChan chan Events

	lg *zap.Logger
}

func (e *Etcd) newWatchChan(ctx context.Context, ch clientv3.WatchChan, filter Filter, logger *zap.Logger) *watchChan {
	ctx, cancel := context.WithCancel(ctx)
	return &watchChan{
		prefixHandler: e,
		ctx:           ctx,
		cancel:        cancel,
		ch:            ch,
		filter:        filter,
		resultChan:    make(chan Events, _watchChanCap),
		lg:            logger,
	}
}

func (w *watchChan) Close() {
	w.cancel()
}

func (w *watchChan) EventChan() <-chan Events {
	return w.resultChan
}

func (w *watchChan) run() {
	defer func() {
		close(w.resultChan)
	}()

	for {
		select {
		case <-w.ctx.Done():
			return
		case resp, ok := <-w.ch:
			if !ok {
				return
			}
			if err := resp.Err(); err != nil {
				w.sendError(err)
				return
			}
			if len(resp.Events) == 0 {
				continue
			}
			events := make([]Event, 0, len(resp.Events))
			for _, e := range resp.Events {
				parsed, err := w.parseEvent(e)
				if err != nil {
					w.sendError(err)
					return
				}
				if w.filter != nil && !w.filter(parsed) {
					continue
				}

				events = append(events, parsed)
			}
			if len(events) == 0 {
				continue
			}
			w.sendEvents(Events{Events: events, Revision: resp.Header.Revision})
		}
	}
}

func (w *watchChan) sendError(err error) {
	logger := w.lg.With(zap.Error(err))
	if errors.Is(err, etcdrpc.ErrCompacted) {
		err = model.ErrKVCompacted
		logger.Warn("watch chan error: compaction")
	} else {
		logger.Error("watch chan error")
	}

	select {
	case w.resultChan <- Events{Error: err}:
	case <-w.ctx.Done():
		logger.Warn("error is dropped due to context done")
	}
}

func (w *watchChan) sendEvents(e Events) {
	logger := w.lg.With(zap.Int("events-cnt", len(e.Events)), zap.Int64("events-revision", e.Revision))
	if len(w.resultChan) == cap(w.resultChan) {
		logger.Warn("watch chan is full", zap.Int("cap", cap(w.resultChan)))
	}
	select {
	case w.resultChan <- e:
		if logger.Core().Enabled(zap.DebugLevel) {
			fields := make([]zap.Field, 0, len(e.Events)*3)
			for i, ev := range e.Events {
				fields = append(fields,
					zap.String(fmt.Sprintf("events-%d-type", i), string(ev.Type)),
					zap.ByteString(fmt.Sprintf("events-%d-key", i), ev.Key),
					zap.ByteString(fmt.Sprintf("events-%d-value", i), ev.Value),
				)
			}
			logger.Debug("send events to watch chan", fields...)
		}
	case <-w.ctx.Done():
		logger.Warn("events are dropped due to context done")
	}
}

func (w *watchChan) parseEvent(e *clientv3.Event) (Event, error) {
	var ret Event
	switch {
	case e.Type == clientv3.EventTypeDelete:
		if e.PrevKv == nil {
			// The previous value has been compacted.
			return Event{}, errors.Errorf("etcd event has no previous key-value pair. key: %q, modRevision: %d, type: %s", e.Kv.Key, e.Kv.ModRevision, e.Type)
		}
		ret = Event{
			Type:  Deleted,
			Key:   w.trimPrefix(e.Kv.Key),
			Value: e.PrevKv.Value,
		}
	case e.IsCreate():
		ret = Event{
			Type:  Added,
			Key:   w.trimPrefix(e.Kv.Key),
			Value: e.Kv.Value,
		}
	case e.IsModify():
		ret = Event{
			Type:  Modified,
			Key:   w.trimPrefix(e.Kv.Key),
			Value: e.Kv.Value,
		}
	default:
		// Should never happen as etcd clientv3.Event has no other types.
	}
	return ret, nil
}

func (e *Etcd) newEtcdTxn(ctx context.Context) *etcdTxn {
	logger := e.lg.With(traceutil.TraceLogField(ctx))
	return &etcdTxn{
		prefixHandler: e,
		lease:         e,
		kv:            e,
		txn:           e.newTxnFunc(ctx),
		lg:            logger,
	}
}

// etcdTxn is a wrapper of BasicKV.
// It stores the results of all read operations in cs.
// It stores requests for all write operations in ops.
// When Commit is called, cs and ops are wrapped and executed within the same transaction.
// In other words, write operations are only executed if the data read remains unmodified.
type etcdTxn struct {
	prefixHandler
	lease
	kv BasicKV

	txn clientv3.Txn
	cs  []clientv3.Cmp
	ops []clientv3.Op

	lg *zap.Logger
}

func (et *etcdTxn) Get(ctx context.Context, k []byte) ([]byte, error) {
	if len(k) == 0 {
		return nil, nil
	}

	v, err := et.kv.Get(ctx, k)
	if err != nil {
		return nil, err
	}

	var c clientv3.Cmp
	if v == nil {
		// key does not exist
		c = clientv3.Compare(clientv3.CreateRevision(string(et.addPrefix(k))), "=", 0)
	} else {
		c = clientv3.Compare(clientv3.Value(string(et.addPrefix(k))), "=", string(v))
	}
	et.cs = append(et.cs, c)

	return v, nil
}

func (et *etcdTxn) Put(ctx context.Context, k, v []byte, _ bool, ttl int64) ([]byte, error) {
	if len(k) == 0 {
		return nil, nil
	}

	var opts []clientv3.OpOption
	if ttl > 0 {
		leaseID, err := et.grantLease(ctx, ttl)
		if err != nil {
			return nil, err
		}
		opts = append(opts, clientv3.WithLease(leaseID))
	}

	op := clientv3.OpPut(string(et.addPrefix(k)), string(v), opts...)
	et.ops = append(et.ops, op)

	return nil, nil
}

func (et *etcdTxn) Delete(_ context.Context, k []byte, _ bool) ([]byte, error) {
	if len(k) == 0 {
		return nil, nil
	}

	op := clientv3.OpDelete(string(et.addPrefix(k)))
	et.ops = append(et.ops, op)

	return nil, nil
}

func (et *etcdTxn) DeleteByRange(_ context.Context, r Range) (int, error) {
	if len(r.StartKey) == 0 || len(r.EndKey) == 0 {
		return 0, nil
	}

	s := et.addPrefix(r.StartKey)
	e := et.addPrefix(r.EndKey)
	op := clientv3.OpDelete(string(s), clientv3.WithRange(string(e)))
	et.ops = append(et.ops, op)

	return 0, nil
}

func (et *etcdTxn) Commit() error {
	if len(et.ops) == 0 {
		return nil
	}

	txn := et.txn.Then(clientv3.OpTxn(et.cs, et.ops, nil))
	resp, err := txn.Commit()
	if err == nil {
		if !resp.Succeeded {
			// Not leader now
			err = model.ErrKVTxnFailed
		} else if !resp.Responses[0].GetResponseTxn().Succeeded {
			// When the transaction succeeds, the number of responses is always 1 and is always a txn response.
			// The data read has been modified
			err = model.ErrKVDataModified
		}
	}

	logger := et.lg
	if logger.Core().Enabled(zap.DebugLevel) {
		var fields []zap.Field
		for i, c := range et.cs {
			fields = append(fields,
				zap.ByteString(fmt.Sprintf("cmp-%d-key", i), c.KeyBytes()),
				zap.Binary(fmt.Sprintf("cmp-%d-value", i), c.ValueBytes()),
			)
		}
		for i, op := range et.ops {
			fields = append(fields, opFields(op, i)...)
		}
		fields = append(fields, zap.Error(err))
		logger.Debug("commit etcd txn", fields...)
	}

	return err
}

func opFields(op clientv3.Op, index int) []zap.Field {
	switch {
	case op.IsPut():
		return []zap.Field{
			zap.String(fmt.Sprintf("op-%d-value", index), "PUT"),
			zap.ByteString(fmt.Sprintf("op-%d-key", index), op.KeyBytes()),
			zap.Binary(fmt.Sprintf("op-%d-value", index), op.ValueBytes()),
		}
	case op.IsDelete():
		return []zap.Field{
			zap.String(fmt.Sprintf("op-%d-value", index), "DELETE"),
			zap.ByteString(fmt.Sprintf("op-%d-key", index), op.KeyBytes()),
		}
	default:
		return []zap.Field{
			zap.String(fmt.Sprintf("op-%d-type", index), "unknown"),
		}
	}
}
