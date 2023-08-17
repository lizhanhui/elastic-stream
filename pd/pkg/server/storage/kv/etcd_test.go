package kv

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"

	"github.com/AutoMQ/pd/pkg/server/model"
	"github.com/AutoMQ/pd/pkg/util/testutil"
)

func TestEtcd_Get(t *testing.T) {
	type args struct {
		key []byte
	}
	tests := []struct {
		name    string
		preset  map[string]string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "get existing key",
			preset: map[string]string{
				"/test/key1": "val1",
			},
			args: args{
				key: []byte("key1"),
			},
			want: []byte("val1"),
		},
		{
			name: "get non-existing key",
			preset: map[string]string{
				"/test/key1": "val1",
			},
			args: args{
				key: []byte("key2"),
			},
			want: nil,
		},
		{
			name: "get empty key",
			preset: map[string]string{
				"/test/":     "val",
				"/test/key1": "val1",
			},
			args: args{
				key: []byte(""),
			},
			want: nil,
		},
		{
			name: "get nil key",
			preset: map[string]string{
				"/test/":     "val",
				"/test/key1": "val1",
			},
			args: args{
				key: nil,
			},
			want: nil,
		},
		{
			name: "get empty value",
			preset: map[string]string{
				"/test/key": "",
			},
			args: args{
				key: []byte("key"),
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)
			_, client, closeFunc := testutil.StartEtcd(t, nil)
			defer closeFunc()

			etcd := Logger{NewEtcd(EtcdParam{
				Client:   client,
				RootPath: "/test",
			}, zap.NewNop())}

			// prepare
			kv := client.KV
			for k, v := range tt.preset {
				_, err := kv.Put(context.Background(), k, v)
				re.NoError(err)
			}

			// run
			got, err := etcd.Get(context.Background(), tt.args.key)

			// check
			if tt.wantErr {
				re.Error(err)
			} else {
				re.NoError(err)
				re.Equal(tt.want, got)
			}
		})
	}
}

func TestEtcd_BatchGet(t *testing.T) {
	type fields struct {
		newCmpFunc func() clientv3.Cmp
		maxTxnOps  uint
	}
	type args struct {
		keys  [][]byte
		inTxn bool
	}
	tests := []struct {
		name    string
		preset  map[string]string
		fields  fields
		args    args
		want    []KeyValue
		wantErr bool
		errMsg  string
	}{
		{
			name: "get keys",
			preset: map[string]string{
				"/test/key1": "val1",
				"/test/key2": "val2",
			},
			args: args{keys: [][]byte{
				[]byte("key1"),
				[]byte("key2"),
			}},
			want: []KeyValue{
				{Key: []byte("key1"), Value: []byte("val1")},
				{Key: []byte("key2"), Value: []byte("val2")},
			},
		},
		{
			name: "get nonexistent key and existing key and empty key",
			preset: map[string]string{
				"/test/":     "val",
				"/test/key1": "val1",
				"/test/key2": "val2",
			},
			args: args{keys: [][]byte{
				[]byte("key0"),
				[]byte("key1"),
				[]byte(""),
			}},
			want: []KeyValue{
				{Key: []byte("key1"), Value: []byte("val1")},
			},
		},
		{
			name: "get with empty list",
			args: args{keys: [][]byte{}},
			want: nil,
		},
		{
			name: "get when transaction failed",
			preset: map[string]string{
				"/test/key1": "val1",
				"/test/key2": "val2",
			},
			fields: fields{
				newCmpFunc: alwaysFailedTxnFunc,
			},
			args: args{keys: [][]byte{
				[]byte("key0"),
				[]byte("key1"),
			}},
			wantErr: true,
			errMsg:  "etcd transaction failed",
		},
		{
			name: "get in multiple transactions",
			preset: map[string]string{
				"/test/key1": "val1",
				"/test/key2": "val2",
			},
			fields: fields{
				maxTxnOps: 1,
			},
			args: args{keys: [][]byte{
				[]byte("key1"),
				[]byte("key2"),
			}},
			want: []KeyValue{
				{Key: []byte("key1"), Value: []byte("val1")},
				{Key: []byte("key2"), Value: []byte("val2")},
			},
		},
		{
			name: "get when exceeding maxTxnOps",
			preset: map[string]string{
				"/test/key1": "val1",
				"/test/key2": "val2",
			},
			fields: fields{
				maxTxnOps: 1,
			},
			args: args{keys: [][]byte{
				[]byte("key1"),
				[]byte("key2"),
			}, inTxn: true},
			wantErr: true,
			errMsg:  "too many txn operations",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)

			var cfg *embed.Config
			if tt.fields.maxTxnOps != 0 {
				cfg = testutil.NewEtcdConfig(t)
				cfg.MaxTxnOps = tt.fields.maxTxnOps
			}
			_, client, closeFunc := testutil.StartEtcd(t, cfg)
			defer closeFunc()

			etcd := Logger{NewEtcd(EtcdParam{
				Client:    client,
				RootPath:  "/test",
				CmpFunc:   tt.fields.newCmpFunc,
				MaxTxnOps: tt.fields.maxTxnOps,
			}, zap.NewNop())}

			// prepare
			kv := client.KV
			for k, v := range tt.preset {
				_, err := kv.Put(context.Background(), k, v)
				re.NoError(err)
			}

			// run
			got, err := etcd.BatchGet(context.Background(), tt.args.keys, tt.args.inTxn)

			// check
			if tt.wantErr {
				re.ErrorContains(err, tt.errMsg)
			} else {
				re.NoError(err)
				re.Equal(tt.want, got)
			}
		})
	}
}

func TestEtcd_GetByRange(t *testing.T) {
	type fields struct {
		newCmpFunc func() clientv3.Cmp
	}
	type args struct {
		r     Range
		rev   int64
		limit int64
		desc  bool
	}
	type want struct {
		kvs      []KeyValue
		revision int64
		more     bool
	}
	type kv struct {
		k, v string
	}
	tests := []struct {
		name    string
		preset  []kv
		fields  fields
		args    args
		want    want
		wantErr bool
		errMsg  string
	}{
		{
			name:   "get keys",
			preset: []kv{{"/test/key1", "val1"}, {"/test/key2", "val2"}, {"/test/key3", "val3"}},
			args: args{
				r: Range{[]byte("key1"), []byte("key3")},
			},
			want: want{kvs: []KeyValue{{Key: []byte("key1"), Value: []byte("val1")}, {Key: []byte("key2"), Value: []byte("val2")}}, revision: 4},
		},
		{
			name:   "get keys with prefix",
			preset: []kv{{"/test/key1", "val1"}, {"/test/key2", "val2"}, {"/test/key3", "val3"}},
			args: args{
				r: Range{[]byte("key"), []byte(clientv3.GetPrefixRangeEnd("key"))},
			},
			want: want{kvs: []KeyValue{{Key: []byte("key1"), Value: []byte("val1")}, {Key: []byte("key2"), Value: []byte("val2")}, {Key: []byte("key3"), Value: []byte("val3")}}, revision: 4},
		},
		{
			name:   "get keys with rev",
			preset: []kv{{"/test/key1", "val1"}, {"/test/key2", "val2"}, {"/test/key3", "val3"}},
			args: args{
				r:   Range{[]byte("key1"), []byte("key3")},
				rev: 3,
			},
			want: want{kvs: []KeyValue{{Key: []byte("key1"), Value: []byte("val1")}, {Key: []byte("key2"), Value: []byte("val2")}}, revision: 3},
		},
		{
			name:   "get keys with limit",
			preset: []kv{{"/test/key1", "val1"}, {"/test/key2", "val2"}, {"/test/key3", "val3"}},
			args: args{
				r:     Range{[]byte("key1"), []byte("key3")},
				limit: 1,
			},
			want: want{kvs: []KeyValue{{Key: []byte("key1"), Value: []byte("val1")}}, revision: 4, more: true},
		},
		{
			name:   "get keys with desc",
			preset: []kv{{"/test/key1", "val1"}, {"/test/key2", "val2"}, {"/test/key3", "val3"}},
			args: args{
				r:    Range{[]byte("key1"), []byte("key3")},
				desc: true,
			},
			want: want{kvs: []KeyValue{{Key: []byte("key2"), Value: []byte("val2")}, {Key: []byte("key1"), Value: []byte("val1")}}, revision: 4},
		},

		{
			name:   "end key greater than start key",
			preset: []kv{{"/test/key1", "val1"}, {"/test/key2", "val2"}, {"/test/key3", "val3"}},
			args: args{
				r: Range{[]byte("key3"), []byte("key1")},
			},
			want: want{kvs: []KeyValue{}, revision: 4},
		},
		{
			name:   "get keys with empty range",
			preset: []kv{{"/test/key1", "val1"}, {"/test/key2", "val2"}, {"/test/key3", "val3"}},
			args: args{
				r: Range{[]byte(""), []byte("")},
			},
			want: want{},
		},
		{
			name:   "get when transaction failed",
			preset: []kv{{"/test/key1", "val1"}, {"/test/key2", "val2"}, {"/test/key3", "val3"}},
			fields: fields{newCmpFunc: alwaysFailedTxnFunc},
			args: args{
				r: Range{[]byte("key1"), []byte("key3")},
			},
			wantErr: true,
			errMsg:  "etcd transaction failed",
		},
		{
			name:   "get with future revision",
			preset: []kv{{"/test/key1", "val1"}, {"/test/key2", "val2"}, {"/test/key3", "val3"}},
			args: args{
				r:   Range{[]byte("key1"), []byte("key3")},
				rev: 5,
			},
			wantErr: true,
			errMsg:  "required revision is a future revision",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)
			_, client, closeFunc := testutil.StartEtcd(t, nil)
			defer closeFunc()

			etcd := Logger{NewEtcd(EtcdParam{
				Client:   client,
				RootPath: "/test",
				CmpFunc:  tt.fields.newCmpFunc,
			}, zap.NewNop())}

			// prepare
			kv := client.KV
			for _, pair := range tt.preset {
				_, err := kv.Put(context.Background(), pair.k, pair.v)
				re.NoError(err)
			}

			// run
			kvs, revision, more, err := etcd.GetByRange(context.Background(), tt.args.r, tt.args.rev, tt.args.limit, tt.args.desc)

			// check
			if tt.wantErr {
				re.ErrorContains(err, tt.errMsg)
			} else {
				re.NoError(err)
				re.Equal(tt.want, want{kvs: kvs, revision: revision, more: more})
			}
		})
	}
}

func TestEtcd_Watch(t *testing.T) {
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()
	type args struct {
		ctx    context.Context
		prefix []byte
		rev    int64
		filter Filter
	}
	type kv struct {
		k, v string
	}
	type want struct {
		Events
		errStr string
	}
	tests := []struct {
		name     string
		preset   []kv
		args     args
		afterOps []func(kv clientv3.KV) error
		wants    []*want
	}{
		{
			name:   "normal case",
			preset: []kv{{"/test/foo/key1", "val1"}, {"/test/bar/key2", "val2"}, {"/test/foo/key3", "val3"}},
			args: args{
				ctx:    context.Background(),
				prefix: []byte("foo/"),
				rev:    3,
			},
			afterOps: []func(kv clientv3.KV) error{
				// Add
				func(kv clientv3.KV) error {
					_, err := kv.Txn(context.Background()).Then(
						clientv3.OpPut("/test/foo/key4", "val4"),
						clientv3.OpPut("/test/foo/key5", "val5")).Commit()
					return err
				},
				// Modify and delete
				func(kv clientv3.KV) error {
					_, err := kv.Txn(context.Background()).Then(
						clientv3.OpPut("/test/foo/key4", "val44"),
						clientv3.OpDelete("/test/foo/key5")).Commit()
					return err
				},
			},
			wants: []*want{
				{Events: Events{
					Events:   []Event{{Type: Added, Key: []byte("foo/key3"), Value: []byte("val3")}},
					Revision: 4,
				}},
				{Events: Events{
					Events:   []Event{{Type: Added, Key: []byte("foo/key4"), Value: []byte("val4")}, {Type: Added, Key: []byte("foo/key5"), Value: []byte("val5")}},
					Revision: 5,
				}},
				{Events: Events{
					Events:   []Event{{Type: Modified, Key: []byte("foo/key4"), Value: []byte("val44")}, {Type: Deleted, Key: []byte("foo/key5"), Value: []byte("val5")}},
					Revision: 6,
				}},
			},
		},
		{
			name:   "filter",
			preset: []kv{},
			args: args{
				ctx:    context.Background(),
				prefix: []byte("foo/"),
				filter: func(e Event) bool {
					if e.Type == Modified {
						return false
					}
					return strings.HasPrefix(string(e.Key), "foo/key4")
				},
			},
			afterOps: []func(kv clientv3.KV) error{
				// Add
				func(kv clientv3.KV) error {
					_, err := kv.Txn(context.Background()).Then(
						clientv3.OpPut("/test/foo/key4", "val4"),
						clientv3.OpPut("/test/foo/key5", "val5")).Commit()
					return err
				},
				// Modify
				func(kv clientv3.KV) error {
					_, err := kv.Txn(context.Background()).Then(
						clientv3.OpPut("/test/foo/key4", "val44"),
						clientv3.OpPut("/test/foo/key5", "val55")).Commit()
					return err
				},
				// Delete
				func(kv clientv3.KV) error {
					_, err := kv.Txn(context.Background()).Then(
						clientv3.OpDelete("/test/foo/key4"),
						clientv3.OpDelete("/test/foo/key5")).Commit()
					return err
				},
				// Add
				func(kv clientv3.KV) error {
					_, err := kv.Txn(context.Background()).Then(
						clientv3.OpPut("/test/foo/key4", "val444"),
						clientv3.OpPut("/test/foo/key5", "val555")).Commit()
					return err
				},
			},
			wants: []*want{
				nil,
				{Events: Events{
					Events:   []Event{{Type: Added, Key: []byte("foo/key4"), Value: []byte("val4")}},
					Revision: 2,
				}},
				nil,
				{Events: Events{
					Events:   []Event{{Type: Deleted, Key: []byte("foo/key4"), Value: []byte("val44")}},
					Revision: 4,
				}},

				{Events: Events{
					Events:   []Event{{Type: Added, Key: []byte("foo/key4"), Value: []byte("val444")}},
					Revision: 5,
				}},
			},
		},
		{
			name:   "timeout context",
			preset: []kv{{"/test/foo/key1", "val1"}, {"/test/bar/key2", "val2"}, {"/test/foo/key3", "val3"}},
			args: args{
				ctx:    timeoutCtx,
				prefix: []byte("foo/"),
				rev:    3,
			},
		},
		{
			name:   "compacted after receiving events",
			preset: []kv{{"/test/foo/key1", "val1"}, {"/test/bar/key2", "val2"}, {"/test/foo/key3", "val3"}},
			args: args{
				ctx:    context.Background(),
				prefix: []byte("foo/"),
				rev:    3,
			},
			afterOps: []func(kv clientv3.KV) error{
				func(kv clientv3.KV) error {
					resp, err := kv.Compact(context.Background(), 4)
					_ = resp
					return err
				},
				func(kv clientv3.KV) error {
					_, err := kv.Txn(context.Background()).Then(
						clientv3.OpPut("/test/foo/key4", "val4"),
						clientv3.OpPut("/test/foo/key5", "val5")).Commit()
					return err
				},
			},
			wants: []*want{
				{Events: Events{
					Events:   []Event{{Type: Added, Key: []byte("foo/key3"), Value: []byte("val3")}},
					Revision: 4,
				}},
				nil,
				{Events: Events{
					Events:   []Event{{Type: Added, Key: []byte("foo/key4"), Value: []byte("val4")}, {Type: Added, Key: []byte("foo/key5"), Value: []byte("val5")}},
					Revision: 5,
				}},
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)
			_, client, closeFunc := testutil.StartEtcd(t, nil)
			defer closeFunc()

			etcd := Logger{NewEtcd(EtcdParam{
				Client:   client,
				RootPath: "/test",
			}, zap.NewNop())}

			// prepare
			kv := client.KV
			for _, pair := range tt.preset {
				_, err := kv.Put(context.Background(), pair.k, pair.v)
				re.NoError(err)
			}

			// run
			watcher := etcd.Watch(tt.args.ctx, tt.args.prefix, tt.args.rev, tt.args.filter)
			defer watcher.Close()

			checkDone := make(chan struct{})
			sig := make(chan struct{})

			go func() {
				defer close(checkDone)
				// check
				ch := watcher.EventChan()
				for i, w := range tt.wants {
					if w == nil {
						// skip
						sig <- struct{}{}
						continue
					}
					select {
					case e, ok := <-ch:
						if !ok {
							re.Failf("channel closed", "did not receive event %d: %+v", i, w)
						}
						if w.errStr != "" {
							re.ErrorContains(e.Error, w.errStr)
							continue
						}
						re.Equal(w.Events, e)
						sig <- struct{}{}
					case <-time.After(1 * time.Second):
						re.Failf("timeout", "did not receive event %d: %+v", i, w)
					}
				}
			}()

			// apply operations
			for _, op := range tt.afterOps {
				select {
				case <-sig:
				case <-checkDone:
					return
				}
				err := op(kv)
				re.NoError(err)
			}
			select {
			case <-sig:
			case <-checkDone:
			}
		})
	}
}

func TestEtcd_WatchCompacted(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	_, client, closeFunc := testutil.StartEtcd(t, nil)
	defer closeFunc()

	etcd := Logger{NewEtcd(EtcdParam{
		Client:   client,
		RootPath: "/test",
	}, zap.NewNop())}

	// put keys and compact
	kv := client.KV
	for i := 0; i < 10; i++ {
		_, err := kv.Put(context.Background(), fmt.Sprintf("/test/foo/key%d", i), fmt.Sprintf("val%d", i))
		re.NoError(err)
	}
	_, err := kv.Compact(context.Background(), 10)
	re.NoError(err)

	// watch a compacted revision
	watcher := etcd.Watch(context.Background(), []byte("foo/"), 1, nil)
	defer watcher.Close()

	event := <-watcher.EventChan()
	re.ErrorContains(event.Error, model.ErrKVCompacted.Error())
}

func TestEtcd_Put(t *testing.T) {
	type args struct {
		key    []byte
		value  []byte
		prevKV bool
	}
	tests := []struct {
		name    string
		preset  map[string]string
		args    args
		want    []byte
		after   map[string]string
		wantErr bool
	}{
		{
			name: "put new key",
			preset: map[string]string{
				"/test/key0": "val0",
			},
			args: args{
				key:    []byte("key1"),
				value:  []byte("val1"),
				prevKV: true,
			},
			want: nil,
			after: map[string]string{
				"/test/key0": "val0",
				"/test/key1": "val1",
			},
		},
		{
			name: "put existing key",
			preset: map[string]string{
				"/test/key1": "val1",
			},
			args: args{
				key:    []byte("key1"),
				value:  []byte("val2"),
				prevKV: true,
			},
			want: []byte("val1"),
			after: map[string]string{
				"/test/key1": "val2",
			},
		},
		{
			name: "put existing key without prevKV",
			preset: map[string]string{
				"/test/key1": "val1",
			},
			args: args{
				key:   []byte("key1"),
				value: []byte("val2"),
			},
			want: nil,
			after: map[string]string{
				"/test/key1": "val2",
			},
		},
		{
			name: "put with empty key",
			preset: map[string]string{
				"/test/":     "val",
				"/test/key1": "val1",
			},
			args: args{
				key:    []byte(""),
				value:  []byte("val2"),
				prevKV: true,
			},
			want: nil,
			after: map[string]string{
				"/test/":     "val",
				"/test/key1": "val1",
			},
		},
		{
			name: "put with nil key",
			preset: map[string]string{
				"/test/":     "val",
				"/test/key1": "val1",
			},
			args: args{
				key:    nil,
				value:  []byte("val2"),
				prevKV: true,
			},
			want: nil,
			after: map[string]string{
				"/test/":     "val",
				"/test/key1": "val1",
			},
		},
		{
			name: "put empty value",
			preset: map[string]string{
				"/test/key1": "val1",
			},
			args: args{
				key:    []byte("key1"),
				value:  []byte(""),
				prevKV: true,
			},
			want: []byte("val1"),
			after: map[string]string{
				"/test/key1": "",
			},
		},
		{
			name: "put nil value",
			preset: map[string]string{
				"/test/key1": "val1",
			},
			args: args{
				key:    []byte("key1"),
				value:  nil,
				prevKV: true,
			},
			want: []byte("val1"),
			after: map[string]string{
				"/test/key1": "",
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)
			_, client, closeFunc := testutil.StartEtcd(t, nil)
			defer closeFunc()

			etcd := Logger{NewEtcd(EtcdParam{
				Client:   client,
				RootPath: "/test",
			}, zap.NewNop())}

			// prepare
			kv := client.KV
			for k, v := range tt.preset {
				_, err := kv.Put(context.Background(), k, v)
				re.NoError(err)
			}

			// run
			got, err := etcd.Put(context.Background(), tt.args.key, tt.args.value, tt.args.prevKV)

			// check
			if tt.wantErr {
				re.Error(err)
			} else {
				re.NoError(err)
				re.Equal(tt.want, got)
			}
			resp, err := kv.Get(context.Background(), "/test", clientv3.WithPrefix())
			re.NoError(err)
			re.Equal(len(tt.after), len(resp.Kvs))
			for _, kvs := range resp.Kvs {
				re.Equal(tt.after[string(kvs.Key)], string(kvs.Value))
			}
		})
	}
}

func TestEtcd_BatchPut(t *testing.T) {
	type fields struct {
		newCmpFunc func() clientv3.Cmp
		maxTxnOps  uint
	}
	type args struct {
		kvs    []KeyValue
		prevKV bool
		inTxn  bool
	}
	tests := []struct {
		name    string
		preset  map[string]string
		fields  fields
		args    args
		want    []KeyValue
		after   map[string]string
		wantErr bool
		errMsg  string
	}{
		{
			name: "put new key",
			preset: map[string]string{
				"/test/key0": "val0",
			},
			args: args{
				kvs: []KeyValue{
					{
						Key:   []byte("key1"),
						Value: []byte("val1"),
					},
					{
						Key:   []byte("key2"),
						Value: []byte("val2"),
					},
				},
				prevKV: true,
			},
			want: []KeyValue{},
			after: map[string]string{
				"/test/key0": "val0",
				"/test/key1": "val1",
				"/test/key2": "val2",
			},
		},
		{
			name: "put existing key",
			preset: map[string]string{
				"/test/key1": "val1",
				"/test/key2": "val2",
			},
			args: args{
				kvs: []KeyValue{
					{
						Key:   []byte("key1"),
						Value: []byte("val10"),
					},
					{
						Key:   []byte("key2"),
						Value: []byte("val20"),
					},
				},
				prevKV: true,
			},
			want: []KeyValue{
				{
					Key:   []byte("key1"),
					Value: []byte("val1"),
				},
				{
					Key:   []byte("key2"),
					Value: []byte("val2"),
				},
			},
			after: map[string]string{
				"/test/key1": "val10",
				"/test/key2": "val20",
			},
		},
		{
			name: "put existing key without prevKV",
			preset: map[string]string{
				"/test/key1": "val1",
				"/test/key2": "val2",
			},
			args: args{
				kvs: []KeyValue{
					{
						Key:   []byte("key1"),
						Value: []byte("val10"),
					},
					{
						Key:   []byte("key2"),
						Value: []byte("val20"),
					},
				},
			},
			want: nil,
			after: map[string]string{
				"/test/key1": "val10",
				"/test/key2": "val20",
			},
		},
		{
			name: "put new key, existing key, empty key, nil key, empty value, nil value",
			preset: map[string]string{
				"/test/":     "val",
				"/test/key0": "val0",
				"/test/key1": "val1",
				"/test/key3": "val3",
				"/test/key4": "val4",
			},
			args: args{
				kvs: []KeyValue{
					{
						Key:   []byte("key1"),
						Value: []byte("val10"),
					},
					{
						Key:   []byte("key2"),
						Value: []byte("val2"),
					},
					{
						Key:   []byte(""),
						Value: []byte("v"),
					},
					{
						Key:   nil,
						Value: []byte("v"),
					},
					{
						Key:   []byte("key3"),
						Value: []byte(""),
					},
					{
						Key:   []byte("key4"),
						Value: nil,
					},
				},
				prevKV: true,
			},
			want: []KeyValue{
				{
					Key:   []byte("key1"),
					Value: []byte("val1"),
				},
				{
					Key:   []byte("key3"),
					Value: []byte("val3"),
				},
				{
					Key:   []byte("key4"),
					Value: []byte("val4"),
				},
			},
			after: map[string]string{
				"/test/":     "val",
				"/test/key0": "val0",
				"/test/key1": "val10",
				"/test/key2": "val2",
				"/test/key3": "",
				"/test/key4": "",
			},
		},
		{
			name: "put with empty list",
			preset: map[string]string{
				"/test/key0": "val0",
			},
			args: args{
				kvs: []KeyValue{},
			},
			want: nil,
			after: map[string]string{
				"/test/key0": "val0",
			},
		},
		{
			name: "put when transaction failed",
			preset: map[string]string{
				"/test/key0": "val0",
			},
			fields: fields{
				newCmpFunc: alwaysFailedTxnFunc,
			},
			args: args{
				kvs: []KeyValue{
					{
						Key:   []byte("key1"),
						Value: []byte("val1"),
					},
				},
			},
			after: map[string]string{
				"/test/key0": "val0",
			},
			wantErr: true,
			errMsg:  "etcd transaction failed",
		},
		{
			name: "put in multiple transactions",
			preset: map[string]string{
				"/test/key1": "val1",
				"/test/key2": "val2",
			},
			fields: fields{
				maxTxnOps: 1,
			},
			args: args{
				kvs: []KeyValue{
					{
						Key:   []byte("key1"),
						Value: []byte("val10"),
					},
					{
						Key:   []byte("key2"),
						Value: []byte("val20"),
					},
				},
				prevKV: true,
			},
			want: []KeyValue{
				{
					Key:   []byte("key1"),
					Value: []byte("val1"),
				},
				{
					Key:   []byte("key2"),
					Value: []byte("val2"),
				},
			},
			after: map[string]string{
				"/test/key1": "val10",
				"/test/key2": "val20",
			},
		},
		{
			name: "put when exceeding maxTxnOps",
			preset: map[string]string{
				"/test/key1": "val1",
				"/test/key2": "val2",
			},
			fields: fields{
				maxTxnOps: 1,
			},
			args: args{
				kvs: []KeyValue{
					{
						Key:   []byte("key1"),
						Value: []byte("val10"),
					},
					{
						Key:   []byte("key2"),
						Value: []byte("val20"),
					},
				},
				prevKV: true,
				inTxn:  true,
			},
			wantErr: true,
			errMsg:  "too many txn operations",
			after: map[string]string{
				"/test/key1": "val1",
				"/test/key2": "val2",
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)

			var cfg *embed.Config
			if tt.fields.maxTxnOps != 0 {
				cfg = testutil.NewEtcdConfig(t)
				cfg.MaxTxnOps = tt.fields.maxTxnOps
			}
			_, client, closeFunc := testutil.StartEtcd(t, cfg)
			defer closeFunc()

			etcd := Logger{NewEtcd(EtcdParam{
				Client:    client,
				RootPath:  "/test",
				CmpFunc:   tt.fields.newCmpFunc,
				MaxTxnOps: tt.fields.maxTxnOps,
			}, zap.NewNop())}

			// prepare
			kv := client.KV
			for k, v := range tt.preset {
				_, err := kv.Put(context.Background(), k, v)
				re.NoError(err)
			}

			// run
			got, err := etcd.BatchPut(context.Background(), tt.args.kvs, tt.args.prevKV, tt.args.inTxn)

			// check
			if tt.wantErr {
				re.ErrorContains(err, tt.errMsg)
			} else {
				re.NoError(err)
				re.Equal(tt.want, got)
			}
			resp, err := kv.Get(context.Background(), "/test", clientv3.WithPrefix())
			re.NoError(err)
			re.Equal(len(tt.after), len(resp.Kvs))
			for _, kvs := range resp.Kvs {
				re.Equal(tt.after[string(kvs.Key)], string(kvs.Value))
			}
		})
	}
}

func TestEtcd_Delete(t *testing.T) {
	type args struct {
		key    []byte
		prevKV bool
	}
	tests := []struct {
		name    string
		preset  map[string]string
		args    args
		want    []byte
		after   map[string]string
		wantErr bool
	}{
		{
			name: "delete existing key",
			preset: map[string]string{
				"/test/key1": "val1",
			},
			args: args{
				key: []byte("key1"),
			},
			want:  nil,
			after: map[string]string{},
		},
		{
			name: "delete existing key and get prevKV",
			preset: map[string]string{
				"/test/key1": "val1",
			},
			args: args{
				key:    []byte("key1"),
				prevKV: true,
			},
			want:  []byte("val1"),
			after: map[string]string{},
		},
		{
			name: "delete nonexistent key",
			preset: map[string]string{
				"/test/key1": "val1",
			},
			args: args{
				key:    []byte("key0"),
				prevKV: true,
			},
			want: nil,
			after: map[string]string{
				"/test/key1": "val1",
			},
		},
		{
			name: "delete key with empty key",
			preset: map[string]string{
				"/test/": "val",
			},
			args: args{
				key:    []byte(""),
				prevKV: true,
			},
			want: nil,
			after: map[string]string{
				"/test/": "val",
			},
		},
		{
			name: "delete key with nil key",
			preset: map[string]string{
				"/test/": "val",
			},
			args: args{
				key:    nil,
				prevKV: true,
			},
			want: nil,
			after: map[string]string{
				"/test/": "val",
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)
			_, client, closeFunc := testutil.StartEtcd(t, nil)
			defer closeFunc()

			etcd := Logger{NewEtcd(EtcdParam{
				Client:   client,
				RootPath: "/test",
			}, zap.NewNop())}

			// prepare
			kv := client.KV
			for k, v := range tt.preset {
				_, err := kv.Put(context.Background(), k, v)
				re.NoError(err)
			}

			// run
			got, err := etcd.Delete(context.Background(), tt.args.key, tt.args.prevKV)

			// check
			if tt.wantErr {
				re.Error(err)
			} else {
				re.NoError(err)
				re.Equal(tt.want, got)
			}
			resp, err := kv.Get(context.Background(), "/test", clientv3.WithPrefix())
			re.NoError(err)
			re.Equal(len(tt.after), len(resp.Kvs))
			for _, kvs := range resp.Kvs {
				re.Equal(tt.after[string(kvs.Key)], string(kvs.Value))
			}
		})
	}
}

func TestEtcd_BatchDelete(t *testing.T) {
	type fields struct {
		newCmpFunc func() clientv3.Cmp
		maxTxnOps  uint
	}
	type args struct {
		keys   [][]byte
		prevKV bool
		inTxn  bool
	}
	tests := []struct {
		name    string
		preset  map[string]string
		fields  fields
		args    args
		want    []KeyValue
		after   map[string]string
		wantErr bool
		errMsg  string
	}{
		{
			name: "delete existing key",
			preset: map[string]string{
				"/test/key1": "val1",
				"/test/key2": "val2",
			},
			args: args{
				keys: [][]byte{[]byte("key1"), []byte("key2")},
			},
			want:  nil,
			after: map[string]string{},
		},
		{
			name: "delete existing key and get prevKV",
			preset: map[string]string{
				"/test/key1": "val1",
				"/test/key2": "val2",
			},
			args: args{
				keys:   [][]byte{[]byte("key1"), []byte("key2")},
				prevKV: true,
			},
			want: []KeyValue{
				{
					Key:   []byte("key1"),
					Value: []byte("val1"),
				},
				{
					Key:   []byte("key2"),
					Value: []byte("val2"),
				},
			},
			after: map[string]string{},
		},
		{
			name: "delete nonexistent key",
			preset: map[string]string{
				"/test/key1": "val1",
				"/test/key2": "val2",
			},
			args: args{
				keys:   [][]byte{[]byte("key0")},
				prevKV: true,
			},
			want: []KeyValue{},
			after: map[string]string{
				"/test/key1": "val1",
				"/test/key2": "val2",
			},
		},
		{
			name: "delete nonexistent key and existing key and empty key",
			preset: map[string]string{
				"/test/":     "val",
				"/test/key1": "val1",
				"/test/key2": "val2",
			},
			args: args{
				keys:   [][]byte{[]byte("key0"), []byte("key1"), []byte(""), nil},
				prevKV: true,
			},
			want: []KeyValue{
				{
					Key:   []byte("key1"),
					Value: []byte("val1"),
				},
			},
			after: map[string]string{
				"/test/":     "val",
				"/test/key2": "val2",
			},
		},
		{
			name: "delete with empty list",
			preset: map[string]string{
				"/test/key0": "val0",
			},
			args: args{
				keys: [][]byte{},
			},
			want: nil,
			after: map[string]string{
				"/test/key0": "val0",
			},
		},
		{
			name: "delete when transaction failed",
			preset: map[string]string{
				"/test/key1": "val1",
				"/test/key2": "val2",
			},
			fields: fields{
				newCmpFunc: alwaysFailedTxnFunc,
			},
			args: args{
				keys:   [][]byte{[]byte("key0"), []byte("key1")},
				prevKV: true,
			},
			after: map[string]string{
				"/test/key1": "val1",
				"/test/key2": "val2",
			},
			wantErr: true,
			errMsg:  "etcd transaction failed",
		},
		{
			name: "delete in multiple transactions",
			preset: map[string]string{
				"/test/key1": "val1",
				"/test/key2": "val2",
			},
			fields: fields{
				maxTxnOps: 1,
			},
			args: args{
				keys:   [][]byte{[]byte("key1"), []byte("key2")},
				prevKV: true,
			},
			want: []KeyValue{
				{
					Key:   []byte("key1"),
					Value: []byte("val1"),
				},
				{
					Key:   []byte("key2"),
					Value: []byte("val2"),
				},
			},
			after: map[string]string{},
		},
		{
			name: "delete when exceeding maxTxnOps",
			preset: map[string]string{
				"/test/key1": "val1",
				"/test/key2": "val2",
			},
			fields: fields{
				maxTxnOps: 1,
			},
			args: args{
				keys:   [][]byte{[]byte("key1"), []byte("key2")},
				prevKV: true,
				inTxn:  true,
			},
			wantErr: true,
			errMsg:  "too many txn operations",
			after: map[string]string{
				"/test/key1": "val1",
				"/test/key2": "val2",
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)

			var cfg *embed.Config
			if tt.fields.maxTxnOps != 0 {
				cfg = testutil.NewEtcdConfig(t)
				cfg.MaxTxnOps = tt.fields.maxTxnOps
			}
			_, client, closeFunc := testutil.StartEtcd(t, cfg)
			defer closeFunc()

			etcd := Logger{NewEtcd(EtcdParam{
				Client:    client,
				RootPath:  "/test",
				CmpFunc:   tt.fields.newCmpFunc,
				MaxTxnOps: tt.fields.maxTxnOps,
			}, zap.NewNop())}

			// prepare
			kv := client.KV
			for k, v := range tt.preset {
				_, err := kv.Put(context.Background(), k, v)
				re.NoError(err)
			}

			// run
			got, err := etcd.BatchDelete(context.Background(), tt.args.keys, tt.args.prevKV, tt.args.inTxn)

			// check
			if tt.wantErr {
				re.ErrorContains(err, tt.errMsg)
			} else {
				re.NoError(err)
				re.Equal(tt.want, got)
			}
			resp, err := kv.Get(context.Background(), "/test", clientv3.WithPrefix())
			re.NoError(err)
			re.Equal(len(tt.after), len(resp.Kvs))
			for _, kvs := range resp.Kvs {
				re.Equal(tt.after[string(kvs.Key)], string(kvs.Value))
			}
		})
	}
}

func TestEtcd_DeleteByPrefix(t *testing.T) {
	type args struct {
		prefixes [][]byte
	}
	tests := []struct {
		name    string
		preset  map[string]string
		args    args
		want    int64
		after   map[string]string
		wantErr bool
	}{
		{
			name: "normal case",
			preset: map[string]string{
				"/test/foo1/key1": "val1",
				"/test/foo1/key2": "val2",
				"/test/foo2/key3": "val3",
				"/test/foo2/key4": "val4",
				"/test/bar/key5":  "val5",
				"/test/bar/key6":  "val6",
			},
			args: args{
				prefixes: [][]byte{[]byte("foo1/"), []byte("foo2/")},
			},
			want: 4,
			after: map[string]string{
				"/test/bar/key5": "val5",
				"/test/bar/key6": "val6",
			},
		},
		{
			name: "delete specific key",
			preset: map[string]string{
				"/test/foo/key1": "val1",
				"/test/foo/key2": "val2",
			},
			args: args{
				prefixes: [][]byte{[]byte("foo/key1")},
			},
			want: 1,
			after: map[string]string{
				"/test/foo/key2": "val2",
			},
		},
		{
			name: "nonexistent prefix",
			preset: map[string]string{
				"/test/foo/key1": "val1",
				"/test/foo/key2": "val2",
			},
			args: args{
				prefixes: [][]byte{[]byte("bar/")},
			},
			want: 0,
			after: map[string]string{
				"/test/foo/key1": "val1",
				"/test/foo/key2": "val2",
			},
		},
		{
			name: "empty key",
			preset: map[string]string{
				"/test/foo/key1": "val1",
			},
			args: args{
				prefixes: [][]byte{[]byte("")},
			},
			want: 0,
			after: map[string]string{
				"/test/foo/key1": "val1",
			},
		},
		{
			name: "nil key",
			preset: map[string]string{
				"/test/foo/key1": "val1",
			},
			args: args{
				prefixes: [][]byte{nil},
			},
			want: 0,
			after: map[string]string{
				"/test/foo/key1": "val1",
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)
			_, client, closeFunc := testutil.StartEtcd(t, nil)
			defer closeFunc()

			etcd := Logger{NewEtcd(EtcdParam{
				Client:   client,
				RootPath: "/test",
			}, zap.NewNop())}

			// prepare
			kv := client.KV
			for k, v := range tt.preset {
				_, err := kv.Put(context.Background(), k, v)
				re.NoError(err)
			}

			// run
			got, err := etcd.DeleteByPrefixes(context.Background(), tt.args.prefixes)

			// check
			if tt.wantErr {
				re.Error(err)
			} else {
				re.NoError(err)
				re.Equal(tt.want, got)
			}
			resp, err := kv.Get(context.Background(), "/test", clientv3.WithPrefix())
			re.NoError(err)
			re.Equal(len(tt.after), len(resp.Kvs))
			for _, kvs := range resp.Kvs {
				re.Equal(tt.after[string(kvs.Key)], string(kvs.Value))
			}
		})
	}
}

func TestEtcd_ExecInTxn(t *testing.T) {
	getAndPut := func(tb testing.TB, kv BasicKV, key string, oldValue []byte, newValue string) {
		re := require.New(tb)
		v, err := kv.Get(context.Background(), []byte(key))
		re.NoError(err)
		re.Equal(oldValue, v)
		v, err = kv.Put(context.Background(), []byte(key), []byte(newValue), false)
		re.NoError(err)
		re.Nil(v)
	}
	getAndDelete := func(tb testing.TB, kv BasicKV, key string, oldValue []byte) {
		re := require.New(tb)
		v, err := kv.Get(context.Background(), []byte(key))
		re.NoError(err)
		re.Equal(oldValue, v)
		v, err = kv.Delete(context.Background(), []byte(key), false)
		re.NoError(err)
		re.Nil(v)
	}

	type fields struct {
		newCmpFunc func() clientv3.Cmp
	}
	type args struct {
		txnFunc func(testing.TB, clientv3.KV, BasicKV) error
	}
	tests := []struct {
		name    string
		preset  map[string]string
		fields  fields
		args    args
		after   map[string]string
		wantErr bool
		errMsg  string
	}{
		{
			name: "normal case",
			preset: map[string]string{
				"/test/key1": "val1",
				"/test/key2": "val2",
			},
			args: args{func(tb testing.TB, _ clientv3.KV, basicKV BasicKV) error {
				// get a non-existent key and put it
				getAndPut(tb, basicKV, "key0", nil, "val0")
				// get an existent key and modify it
				getAndPut(tb, basicKV, "key1", []byte("val1"), "val11")
				// get an existent key and delete it
				getAndDelete(tb, basicKV, "key2", []byte("val2"))
				return nil
			}},
			after: map[string]string{
				"/test/key0": "val0",
				"/test/key1": "val11",
			},
		},
		{
			name: "empty keys",
			preset: map[string]string{
				"/test/key1": "val1",
				"/test/key2": "val2",
			},
			args: args{func(tb testing.TB, _ clientv3.KV, basicKV BasicKV) error {
				getAndPut(tb, basicKV, "", nil, "bar")
				getAndDelete(tb, basicKV, "", nil)
				return nil
			}},
			after: map[string]string{
				"/test/key1": "val1",
				"/test/key2": "val2",
			},
		},
		{
			name: "do nothing",
			preset: map[string]string{
				"/test/key1": "val1",
				"/test/key2": "val2",
			},
			args: args{func(tb testing.TB, _ clientv3.KV, basicKV BasicKV) error {
				return nil
			}},
			after: map[string]string{
				"/test/key1": "val1",
				"/test/key2": "val2",
			},
		},
		{
			name: "data modified by other (three times)",
			preset: map[string]string{
				"/test/key": "0",
			},
			args: args{func(tb testing.TB, kv clientv3.KV, basicKV BasicKV) error {
				re := require.New(tb)
				v, err := basicKV.Get(context.Background(), []byte("key"))
				re.NoError(err)
				times, err := strconv.Atoi(string(v))
				re.NoError(err)
				_, _ = basicKV.Put(context.Background(), []byte("key"), []byte(strconv.Itoa(times+1)), false)

				_, err = kv.Put(context.Background(), "/test/key", strconv.Itoa(times+2))
				re.NoError(err)
				return nil
			}},
			after: map[string]string{
				"/test/key": "6",
			},
			wantErr: true,
			errMsg:  "data has been modified",
		},
		{
			name: "data modified by other (only once)",
			preset: map[string]string{
				"/test/key": "0",
			},
			args: args{func(tb testing.TB, kv clientv3.KV, basicKV BasicKV) error {
				re := require.New(tb)
				v, err := basicKV.Get(context.Background(), []byte("key"))
				re.NoError(err)
				times, err := strconv.Atoi(string(v))
				re.NoError(err)
				_, _ = basicKV.Put(context.Background(), []byte("key"), []byte(strconv.Itoa(times+1)), false)

				if times == 0 {
					_, err = kv.Put(context.Background(), "/test/key", strconv.Itoa(times+2))
					re.NoError(err)
				}
				return nil
			}},
			after: map[string]string{
				"/test/key": "3",
			},
		},
		{
			name: "pass the error returned by f",
			preset: map[string]string{
				"/test/key1": "val1",
				"/test/key2": "val2",
			},
			args: args{func(tb testing.TB, _ clientv3.KV, basicKV BasicKV) error {
				return errors.New("returned by user")
			}},
			after: map[string]string{
				"/test/key1": "val1",
				"/test/key2": "val2",
			},
			wantErr: true,
			errMsg:  "returned by user",
		},
		{
			name: "not leader when get",
			preset: map[string]string{
				"/test/key1": "val1",
				"/test/key2": "val2",
			},
			fields: fields{alwaysFailedTxnFunc},
			args: args{func(tb testing.TB, _ clientv3.KV, basicKV BasicKV) error {
				_, err := basicKV.Get(context.Background(), []byte("key1"))
				return err
			}},
			after: map[string]string{
				"/test/key1": "val1",
				"/test/key2": "val2",
			},
			wantErr: true,
			errMsg:  "etcd transaction failed",
		},
		{
			name: "not leader when commit",
			preset: map[string]string{
				"/test/key1": "val1",
				"/test/key2": "val2",
			},
			fields: fields{alwaysFailedTxnFunc},
			args: args{func(tb testing.TB, _ clientv3.KV, basicKV BasicKV) error {
				_, err := basicKV.Delete(context.Background(), []byte("key2"), false)
				require.NoError(tb, err)
				return nil
			}},
			after: map[string]string{
				"/test/key1": "val1",
				"/test/key2": "val2",
			},
			wantErr: true,
			errMsg:  "etcd transaction failed",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)
			_, client, closeFunc := testutil.StartEtcd(t, nil)
			defer closeFunc()

			etcd := Logger{NewEtcd(EtcdParam{
				Client:   client,
				RootPath: "/test",
				CmpFunc:  tt.fields.newCmpFunc,
			}, zap.NewNop())}

			// prepare
			kv := client.KV
			for k, v := range tt.preset {
				_, err := kv.Put(context.Background(), k, v)
				re.NoError(err)
			}

			// run
			err := etcd.ExecInTxn(context.Background(), func(kv BasicKV) error {
				return tt.args.txnFunc(t, client, kv)
			})

			// check
			if tt.wantErr {
				re.ErrorContains(err, tt.errMsg)
			} else {
				re.NoError(err)
			}
			resp, err := kv.Get(context.Background(), "/test", clientv3.WithPrefix())
			re.NoError(err)
			re.Equal(len(tt.after), len(resp.Kvs))
			for _, kvs := range resp.Kvs {
				re.Equal(tt.after[string(kvs.Key)], string(kvs.Value))
			}
		})
	}
}

func alwaysFailedTxnFunc() clientv3.Cmp {
	return clientv3.Compare(clientv3.Value("key-should-not-be-set"), "=", "value-should-not-be-set")
}
