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

package config

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"

	"github.com/AutoMQ/placement-manager/pkg/util/typeutil"
)

const (
	URLSeparator = "," // URLSeparator is the separator in fields such as PeerUrls, ClientUrls, etc.

	_defaultClientUrls                        = "http://127.0.0.1:2379"
	_defaultPeerUrls                          = "http://127.0.0.1:2380"
	_defaultNameFormat                        = "PM-%s"
	_defaultDataDirFormat                     = "default.%s"
	_defaultInitialClusterPrefix              = "pm="
	_defaultEnableGRPCGateway                 = true
	_defaultInitialClusterToken               = "pm-cluster"
	_defaultLeaderLease                 int64 = 3
	_defaultLeaderPriorityCheckInterval       = time.Minute
)

// Config is the configuration for [Server]
type Config struct {
	v *viper.Viper

	etcd *embed.Config

	PeerUrls            string
	ClientUrls          string
	AdvertisePeerUrls   string
	AdvertiseClientUrls string

	Name           string
	DataDir        string
	InitialCluster string

	// LeaderLease time, if leader doesn't update its TTL
	// in etcd after lease time, etcd will expire the leader key
	// and other servers can campaign the leader again.
	// Etcd only supports seconds TTL, so here is second too.
	LeaderLease int64

	LeaderPriorityCheckInterval typeutil.Duration

	lg *zap.Logger
}

// NewConfig creates a new config.
func NewConfig(arguments []string) (*Config, error) {
	cfg := &Config{}
	cfg.etcd = embed.NewConfig()

	v, fs := configure()

	// parse from command line
	fs.String("config", "", "configuration file")
	err := fs.Parse(arguments)
	if err != nil {
		return nil, err
	}

	// read configuration from file
	c, _ := fs.GetString("config")
	v.SetConfigFile(c)
	err = v.ReadInConfig()
	if err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, errors.Wrap(err, "read configuration file")
		}
	}

	// set config
	err = v.Unmarshal(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal configuration")
	}

	// TODO new and set logger
	logger := cfg.lg

	if configFile := v.ConfigFileUsed(); configFile != "" {
		logger.Info("load configuration from file.", zap.String("file-name", configFile))
	}

	cfg.v = v
	return cfg, nil
}

// Adjust generates default values for some fields (if they are empty)
func (c *Config) Adjust() error {
	if c.AdvertisePeerUrls == "" {
		c.AdvertisePeerUrls = c.PeerUrls
	}
	if c.AdvertiseClientUrls == "" {
		c.AdvertiseClientUrls = c.ClientUrls
	}
	if c.Name == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return errors.Wrap(err, "get hostname")
		}
		c.Name = fmt.Sprintf(_defaultNameFormat, hostname)
	}
	if c.DataDir == "" {
		c.DataDir = fmt.Sprintf(_defaultDataDirFormat, c.Name)
	}
	if c.InitialCluster == "" {
		// For example, when AdvertisePeerUrls is set to "http://127.0.0.1:2380,http://127.0.0.1:2381",
		// the InitialCluster is "pm=http://127.0.0.1:2380,pm=http://127.0.0.1:2381".
		items := strings.Split(c.AdvertisePeerUrls, URLSeparator)
		initialCluster := strings.Join(items, URLSeparator+_defaultInitialClusterPrefix)
		c.InitialCluster += _defaultInitialClusterPrefix + initialCluster
	}
	return nil
}

// Validate checks whether the configuration is valid. It should be called after Adjust
func (c *Config) Validate() error {
	_, err := filepath.Abs(c.DataDir)
	if err != nil {
		return errors.Wrap(err, "invalid data dir path")
	}
	return nil
}

// Etcd generates a configuration for embedded etcd.
func (c *Config) Etcd() (*embed.Config, error) {
	cfg := c.etcd
	cfg.Name = c.Name
	cfg.Dir = c.DataDir
	cfg.InitialCluster = c.InitialCluster
	cfg.EnablePprof = true // TODO

	var err error
	cfg.LPUrls, err = parseUrls(c.PeerUrls)
	if err != nil {
		return nil, errors.Wrap(err, "parse peer url")
	}
	cfg.LCUrls, err = parseUrls(c.ClientUrls)
	if err != nil {
		return nil, errors.Wrap(err, "parse client url")
	}
	cfg.APUrls, err = parseUrls(c.AdvertisePeerUrls)
	if err != nil {
		return nil, errors.Wrap(err, "parse advertise peer url")
	}
	cfg.ACUrls, err = parseUrls(c.AdvertiseClientUrls)
	if err != nil {
		return nil, errors.Wrap(err, "parse advertise client url")
	}

	return cfg, nil
}

// Logger returns logger generated based on the config
func (c *Config) Logger() *zap.Logger {
	return c.lg
}

func configure() (*viper.Viper, *pflag.FlagSet) {
	v := viper.New()
	fs := pflag.NewFlagSet("PM", pflag.ContinueOnError)

	// Viper settings
	v.AddConfigPath(".")
	v.AddConfigPath("$CONFIG_DIR/")

	// etcd urls settings
	fs.String("peer-urls", _defaultPeerUrls, "urls for peer traffic")
	fs.String("client-urls", _defaultClientUrls, "urls for client traffic")
	fs.String("advertise-peer-urls", "", "advertise urls for peer traffic (default '${peer-urls}')")
	fs.String("advertise-client-urls", "", "advertise urls for client traffic (default '${client-urls}')")
	_ = v.BindPFlag("peer-urls", fs.Lookup("peer-urls"))
	_ = v.BindPFlag("client-urls", fs.Lookup("client-urls"))
	_ = v.BindPFlag("advertise-peer-urls", fs.Lookup("advertise-peer-urls"))
	_ = v.BindPFlag("advertise-client-urls", fs.Lookup("advertise-client-urls"))
	v.RegisterAlias("PeerUrls", "peer-urls")
	v.RegisterAlias("ClientUrls", "client-urls")
	v.RegisterAlias("AdvertisePeerUrls", "advertise-peer-urls")
	v.RegisterAlias("AdvertiseClientUrls", "advertise-client-urls")

	// PM members settings
	fs.String("name", "", "human-readable name for this PM member (default 'PM-${hostname}')")
	fs.String("data-dir", "", "path to the data directory (default 'default.${name}')")
	fs.String("initial-cluster", "", "initial cluster configuration for bootstrapping, e.g. pm=http://127.0.0.1:2380. (default 'pm=${advertise-peer-urls}')")
	fs.Int64("leader-lease", _defaultLeaderLease, "expiration time of the leader, in seconds")
	fs.Duration("leader-priority-check-interval", _defaultLeaderPriorityCheckInterval, "time interval for checking the leader's priority")
	_ = v.BindPFlag("name", fs.Lookup("name"))
	_ = v.BindPFlag("data-dir", fs.Lookup("data-dir"))
	_ = v.BindPFlag("initial-cluster", fs.Lookup("initial-cluster"))
	_ = v.BindPFlag("leader-lease", fs.Lookup("leader-lease"))
	_ = v.BindPFlag("leader-priority-check-interval", fs.Lookup("leader-priority-check-interval"))
	v.RegisterAlias("DataDir", "data-dir")
	v.RegisterAlias("InitialCluster", "initial-cluster")
	v.RegisterAlias("LeaderLease", "lease-lease")
	v.RegisterAlias("LeaderPriorityCheckInterval", "leader-priority-check-interval")
	v.SetDefault("etcd.initialClusterToken", _defaultInitialClusterToken)
	v.SetDefault("etcd.enableGRPCGateway", _defaultEnableGRPCGateway)

	return v, fs
}

// parseUrls parse a string into multiple urls.
func parseUrls(s string) ([]url.URL, error) {
	items := strings.Split(s, URLSeparator)
	urls := make([]url.URL, 0, len(items))
	for _, item := range items {
		u, err := url.Parse(item)
		if err != nil {
			return nil, errors.Wrapf(err, "parse url %s", item)
		}

		urls = append(urls, *u)
	}

	return urls, nil
}
