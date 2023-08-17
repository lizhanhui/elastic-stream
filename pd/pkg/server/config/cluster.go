package config

import (
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

const (
	_defaultSealReqTimeoutMs   int32 = 1000
	_defaultRangeServerTimeout       = 100 * time.Second
	_defaultStreamDeleteDelay        = 24 * time.Hour
)

// Cluster is the configuration for cluster.RaftCluster
type Cluster struct {
	SealReqTimeoutMs int32
	// RangeServerTimeout is the timeout after which a range server is considered dead.
	RangeServerTimeout time.Duration
	// StreamDeleteDelay is the time interval between soft deletion and hard deletion of a stream.
	// 0 means never hard delete.
	StreamDeleteDelay time.Duration
}

func NewCluster() *Cluster {
	return &Cluster{}
}

func DefaultCluster() *Cluster {
	return &Cluster{
		SealReqTimeoutMs:   _defaultSealReqTimeoutMs,
		RangeServerTimeout: _defaultRangeServerTimeout,
		StreamDeleteDelay:  _defaultStreamDeleteDelay,
	}
}

func (c *Cluster) Validate() error {
	if c.SealReqTimeoutMs <= 0 {
		return errors.Errorf("invalid seal request timeout `%d`", c.SealReqTimeoutMs)
	}
	if c.RangeServerTimeout <= 0 {
		return errors.Errorf("invalid range server timeout `%s`", c.RangeServerTimeout)
	}
	if c.StreamDeleteDelay < 0 {
		return errors.Errorf("invalid stream delete delay `%s`", c.StreamDeleteDelay)
	}
	return nil
}

func clusterConfigure(v *viper.Viper, fs *pflag.FlagSet) {
	fs.Int32("cluster-seal-req-timeout-ms", _defaultSealReqTimeoutMs, "seal request timeout in milliseconds")
	fs.Duration("cluster-range-server-timeout", _defaultRangeServerTimeout, "timeout after which a range server is considered dead")
	fs.Duration("cluster-stream-delete-delay", _defaultStreamDeleteDelay, "time interval between soft deletion and hard deletion of a stream")
	_ = v.BindPFlag("cluster.sealReqTimeoutMs", fs.Lookup("cluster-seal-req-timeout-ms"))
	_ = v.BindPFlag("cluster.rangeServerTimeout", fs.Lookup("cluster-range-server-timeout"))
	_ = v.BindPFlag("cluster.streamDeleteDelay", fs.Lookup("cluster-stream-delete-delay"))
}
