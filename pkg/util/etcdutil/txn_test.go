package etcdutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type MockEtcdTxn struct {
	mock.Mock
}

func (m *MockEtcdTxn) If(cs ...clientv3.Cmp) clientv3.Txn {
	args := m.Called(cs)
	return args.Get(0).(clientv3.Txn)
}

func (m *MockEtcdTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	args := m.Called(ops)
	return args.Get(0).(clientv3.Txn)
}

func (m *MockEtcdTxn) Else(ops ...clientv3.Op) clientv3.Txn {
	args := m.Called(ops)
	return args.Get(0).(clientv3.Txn)
}

func (m *MockEtcdTxn) Commit() (*clientv3.TxnResponse, error) {
	args := m.Called()
	return args.Get(0).(*clientv3.TxnResponse), args.Error(1)
}

func TestSlowTxn(t *testing.T) {
	t.Parallel()
	re := require.New(t)

	obsZapCore, obsLogs := observer.New(zap.InfoLevel)
	obsLogger := zap.New(obsZapCore)

	mTxn := &MockEtcdTxn{}
	mTxn.On("Commit").After(DefaultSlowRequestTime+time.Second).Return(&clientv3.TxnResponse{}, nil)
	txn := Txn{
		Txn:    mTxn,
		cancel: func() {},
		lg:     obsLogger,
	}

	_, err := txn.Commit()
	re.NoError(err)
	re.Equal(1, obsLogs.FilterMessage("txn runs too slow").Len())
}

func TestNormalTxn(t *testing.T) {
	t.Parallel()
	re := require.New(t)

	_, client, closeFunc := startEtcd(re, t)
	defer closeFunc()

	txn := NewTxn(client)
	_, _ = txn.If(clientv3.Compare(clientv3.CreateRevision("test/key"), "=", 0)).
		Then(clientv3.OpPut("test/key", "val1")).
		Else(clientv3.OpPut("test/key", "val2")).
		Commit()
	got, err := GetOne(client, []byte("test/key"))
	re.NoError(err)
	re.Equal("val1", string(got.Value))

	txn = NewTxn(client)
	_, _ = txn.If(clientv3.Compare(clientv3.CreateRevision("test/key"), "=", 0)).
		Then(clientv3.OpPut("test/key", "val1")).
		Else(clientv3.OpPut("test/key", "val2")).
		Commit()
	got, err = GetOne(client, []byte("test/key"))
	re.NoError(err)
	re.Equal("val2", string(got.Value))
}
