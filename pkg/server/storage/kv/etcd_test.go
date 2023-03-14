package kv

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/goleak"
	"go.uber.org/zap"

	"github.com/AutoMQ/placement-manager/pkg/util/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
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
			_, client, closeFunc := testutil.StartEtcd(re, t)
			defer closeFunc()

			etcd := NewEtcd(client, "/test", zap.NewNop(), nil)

			// prepare
			kv := client.KV
			for k, v := range tt.preset {
				_, err := kv.Put(context.Background(), k, v)
				re.NoError(err)
			}

			// run
			got, err := etcd.Put(tt.args.key, tt.args.value, tt.args.prevKV)

			// check
			if tt.wantErr {
				re.Error(err)
			} else {
				re.NoError(err)
				re.Equal(tt.want, got)
				for k, v := range tt.after {
					resp, err := kv.Get(context.Background(), k)
					re.NoError(err)
					re.Equal(1, len(resp.Kvs))
					re.Equal(v, string(resp.Kvs[0].Value))
				}
			}
		})
	}
}

func TestEtcd_BatchPut(t *testing.T) {
	type fields struct {
		newCmpFunc func() clientv3.Cmp
	}
	type args struct {
		kvs    []KeyValue
		prevKV bool
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
			name: "put when transaction fails",
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
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)
			_, client, closeFunc := testutil.StartEtcd(re, t)
			defer closeFunc()

			etcd := NewEtcd(client, "/test", zap.NewNop(), tt.fields.newCmpFunc)

			// prepare
			kv := client.KV
			for k, v := range tt.preset {
				_, err := kv.Put(context.Background(), k, v)
				re.NoError(err)
			}

			// run
			got, err := etcd.BatchPut(tt.args.kvs, tt.args.prevKV)

			// check
			if tt.wantErr {
				re.ErrorContains(err, tt.errMsg)
			} else {
				re.NoError(err)
				re.Equal(tt.want, got)
			}
			for k, v := range tt.after {
				resp, err := kv.Get(context.Background(), k)
				re.NoError(err)
				re.Equal(1, len(resp.Kvs))
				re.Equal(v, string(resp.Kvs[0].Value))
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
			want: nil,
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
			want: []byte("val1"),
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
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)
			_, client, closeFunc := testutil.StartEtcd(re, t)
			defer closeFunc()

			etcd := NewEtcd(client, "/test", zap.NewNop(), nil)

			// prepare
			kv := client.KV
			for k, v := range tt.preset {
				_, err := kv.Put(context.Background(), k, v)
				re.NoError(err)
			}

			// run
			got, err := etcd.Delete(tt.args.key, tt.args.prevKV)

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

func TestEtcd_BatchDelete(t *testing.T) {
	type fields struct {
		newCmpFunc func() clientv3.Cmp
	}
	type args struct {
		keys   [][]byte
		prevKV bool
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
			name: "delete existing key",
			preset: map[string]string{
				"/test/key1": "val1",
				"/test/key2": "val2",
			},
			args: args{
				keys: [][]byte{[]byte("key1"), []byte("key2")},
			},
			want: nil,
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
		},
		{
			name: "delete when transaction fails",
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
			wantErr: true,
			errMsg:  "etcd transaction failed",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)
			_, client, closeFunc := testutil.StartEtcd(re, t)
			defer closeFunc()

			etcd := NewEtcd(client, "/test", zap.NewNop(), tt.fields.newCmpFunc)

			// prepare
			kv := client.KV
			for k, v := range tt.preset {
				_, err := kv.Put(context.Background(), k, v)
				re.NoError(err)
			}

			// run
			got, err := etcd.BatchDelete(tt.args.keys, tt.args.prevKV)

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

func alwaysFailedTxnFunc() clientv3.Cmp {
	return clientv3.Compare(clientv3.Value("key-should-not-be-set"), "=", "value-should-not-be-set")
}
