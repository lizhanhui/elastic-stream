package config

import (
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

const (
	_defaultSealReqTimeoutMs int32 = 1000
)

// Cluster is the configuration for cluster.RaftCluster
type Cluster struct {
	SealReqTimeoutMs int32
}

func NewCluster() *Cluster {
	return &Cluster{}
}

func (c *Cluster) Validate() error {
	if c.SealReqTimeoutMs <= 0 {
		return errors.Errorf("invalid seal request timeout `%d`", c.SealReqTimeoutMs)
	}
	return nil
}

func clusterConfigure(v *viper.Viper, fs *pflag.FlagSet) {
	fs.Int32("cluster-seal-req-timeout-ms", _defaultSealReqTimeoutMs, "seal request timeout in milliseconds")
	_ = v.BindPFlag("cluster.sealReqTimeoutMs", fs.Lookup("cluster-seal-req-timeout-ms"))
}
