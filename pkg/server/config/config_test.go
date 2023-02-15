package config

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/embed"
)

func TestNewConfig(t *testing.T) {
	type args struct {
		arguments []string
	}
	tests := []struct {
		name    string
		args    args
		want    Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "default config",
			args: args{arguments: []string{}},
			want: Config{
				Etcd: func() *embed.Config {
					config := embed.NewConfig()
					config.InitialClusterToken = "pm-cluster"
					return config
				}(),
				PeerUrls:                    "http://127.0.0.1:2380",
				ClientUrls:                  "http://127.0.0.1:2379",
				AdvertisePeerUrls:           "",
				AdvertiseClientUrls:         "",
				Name:                        "",
				DataDir:                     "",
				InitialCluster:              "",
				LeaderLease:                 3,
				LeaderPriorityCheckInterval: time.Minute,
			},
		},
		{
			name: "config from command line",
			args: args{arguments: []string{
				"--peer-urls=test-peer-urls",
				"--client-urls=test-client-urls",
				"--advertise-peer-urls=test-advertise-peer-urls",
				"--advertise-client-urls=test-advertise-client-urls",
				"--name=test-name",
				"--data-dir=test-data-dir",
				"--initial-cluster=test-initial-cluster",
				"--leader-lease=123",
				"--leader-priority-check-interval=1h1m1s",
			}},
			want: Config{
				Etcd: func() *embed.Config {
					config := embed.NewConfig()
					config.InitialClusterToken = "pm-cluster"
					return config
				}(),
				PeerUrls:                    "test-peer-urls",
				ClientUrls:                  "test-client-urls",
				AdvertisePeerUrls:           "test-advertise-peer-urls",
				AdvertiseClientUrls:         "test-advertise-client-urls",
				Name:                        "test-name",
				DataDir:                     "test-data-dir",
				InitialCluster:              "test-initial-cluster",
				LeaderLease:                 123,
				LeaderPriorityCheckInterval: time.Hour + time.Minute + time.Second,
			},
		},
		{
			name: "config from toml file",
			args: args{arguments: []string{
				"--config=./test/test-config.toml",
			}},
			want: Config{
				Etcd: func() *embed.Config {
					config := embed.NewConfig()
					config.InitialClusterToken = "test-initial-cluster-token"
					return config
				}(),
				PeerUrls:                    "test-peer-urls",
				ClientUrls:                  "test-client-urls",
				AdvertisePeerUrls:           "test-advertise-peer-urls",
				AdvertiseClientUrls:         "test-advertise-client-urls",
				Name:                        "test-name",
				DataDir:                     "test-data-dir",
				InitialCluster:              "test-initial-cluster",
				LeaderLease:                 123,
				LeaderPriorityCheckInterval: time.Hour + time.Minute + time.Second,
			},
		},
		{
			name: "config from yaml file",
			args: args{arguments: []string{
				"--config=./test/test-config.yaml",
			}},
			want: Config{
				Etcd: func() *embed.Config {
					config := embed.NewConfig()
					config.InitialClusterToken = "test-initial-cluster-token"
					return config
				}(),
				PeerUrls:                    "test-peer-urls",
				ClientUrls:                  "test-client-urls",
				AdvertisePeerUrls:           "test-advertise-peer-urls",
				AdvertiseClientUrls:         "test-advertise-client-urls",
				Name:                        "test-name",
				DataDir:                     "test-data-dir",
				InitialCluster:              "test-initial-cluster",
				LeaderLease:                 123,
				LeaderPriorityCheckInterval: time.Hour + time.Minute + time.Second,
			},
		},
		{
			name: "help message",
			args: args{arguments: []string{
				"--help",
			}},
			wantErr: true,
			errMsg:  pflag.ErrHelp.Error(),
		},
		{
			name: "parse arguments error",
			args: args{arguments: []string{
				"--name=test",
				"--peer-urls",
			}},
			wantErr: true,
			errMsg:  "flag needs an argument",
		},
		{
			name: "read configuration file error",
			args: args{arguments: []string{
				"--config=not-exist.yaml",
			}},
			wantErr: true,
			errMsg:  "read configuration file",
		},
		{
			name: "unmarshal configuration error",
			args: args{arguments: []string{
				"--config=./test/test-invalid.toml",
			}},
			wantErr: true,
			errMsg:  "unmarshal configuration",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)

			config, err := NewConfig(tt.args.arguments)

			if tt.wantErr {
				re.ErrorContains(err, tt.errMsg)
				return
			}
			re.NoError(err)
			// do not check auxiliary fields
			config.v = nil
			config.lg = nil
			re.Equal(tt.want, *config)
		})
	}
}

func TestAdjust(t *testing.T) {
	// TODO
	_ = t
	hostname, _ := os.Hostname()
	_ = Config{
		Etcd: func() *embed.Config {
			config := embed.NewConfig()
			config.InitialClusterToken = "pm-cluster"
			return config
		}(),
		PeerUrls:                    "http://127.0.0.1:2380",
		ClientUrls:                  "http://127.0.0.1:2379",
		AdvertisePeerUrls:           "http://127.0.0.1:2380",
		AdvertiseClientUrls:         "http://127.0.0.1:2379",
		Name:                        fmt.Sprintf("pm-%s", hostname),
		DataDir:                     fmt.Sprintf("default.pm-%s", hostname),
		InitialCluster:              "pm=http://127.0.0.1:2380",
		LeaderLease:                 3,
		LeaderPriorityCheckInterval: time.Minute,
	}
}
