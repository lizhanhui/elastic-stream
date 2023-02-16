package config

import (
	"fmt"
	"net/url"
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
	hostname, e := os.Hostname()
	require.NoError(t, e)

	tests := []struct {
		name    string
		in      *Config
		want    *Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "default config",
			in:   func() *Config { config, _ := NewConfig([]string{}); return config }(),
			want: &Config{
				Etcd: func() *embed.Config {
					config := embed.NewConfig()
					config.Name = fmt.Sprintf("pm-%s", hostname)
					config.Dir = fmt.Sprintf("default.pm-%s", hostname)
					config.InitialCluster = "pm=http://127.0.0.1:2380"
					config.LPUrls, _ = parseUrls("http://127.0.0.1:2380")
					config.LCUrls, _ = parseUrls("http://127.0.0.1:2379")
					config.APUrls, _ = parseUrls("http://127.0.0.1:2380")
					config.ACUrls, _ = parseUrls("http://127.0.0.1:2379")
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
			},
		},
		{
			name: "normal adjust config",
			in: func() *Config {
				config, _ := NewConfig([]string{})
				config.PeerUrls = "http://example.com:2380,http://10.0.0.1:2380"
				config.ClientUrls = "http://example.com:2379,http://10.0.0.1:2379"
				config.Name = "test-name"
				return config
			}(),
			want: &Config{
				Etcd: func() *embed.Config {
					config := embed.NewConfig()
					config.Name = "test-name"
					config.Dir = "default.test-name"
					config.InitialCluster = "pm=http://example.com:2380,pm=http://10.0.0.1:2380"
					config.LPUrls, _ = parseUrls("http://example.com:2380,http://10.0.0.1:2380")
					config.LCUrls, _ = parseUrls("http://example.com:2379,http://10.0.0.1:2379")
					config.APUrls, _ = parseUrls("http://example.com:2380,http://10.0.0.1:2380")
					config.ACUrls, _ = parseUrls("http://example.com:2379,http://10.0.0.1:2379")
					config.InitialClusterToken = "pm-cluster"
					return config
				}(),
				PeerUrls:                    "http://example.com:2380,http://10.0.0.1:2380",
				ClientUrls:                  "http://example.com:2379,http://10.0.0.1:2379",
				AdvertisePeerUrls:           "http://example.com:2380,http://10.0.0.1:2380",
				AdvertiseClientUrls:         "http://example.com:2379,http://10.0.0.1:2379",
				Name:                        "test-name",
				DataDir:                     "default.test-name",
				InitialCluster:              "pm=http://example.com:2380,pm=http://10.0.0.1:2380",
				LeaderLease:                 3,
				LeaderPriorityCheckInterval: time.Minute,
			},
		},
		{
			name: "invalid peer url",
			in: func() *Config {
				config, _ := NewConfig([]string{})
				config.PeerUrls = "http://example.com:2380,://10.0.0.1:2380"
				return config
			}(),
			wantErr: true,
			errMsg:  "parse peer url",
		},
		{
			name: "invalid client url",
			in: func() *Config {
				config, _ := NewConfig([]string{})
				config.ClientUrls = "http://example.com:2379,://10.0.0.1:2379"
				return config
			}(),
			wantErr: true,
			errMsg:  "parse client url",
		},
		{
			name: "invalid advertise peer url",
			in: func() *Config {
				config, _ := NewConfig([]string{})
				config.AdvertisePeerUrls = "http://example.com:2380,://10.0.0.1:2380"
				return config
			}(),
			wantErr: true,
			errMsg:  "parse advertise peer url",
		},
		{
			name: "invalid client url",
			in: func() *Config {
				config, _ := NewConfig([]string{})
				config.AdvertiseClientUrls = "http://example.com:2379,://10.0.0.1:2379"
				return config
			}(),
			wantErr: true,
			errMsg:  "parse advertise client url",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)

			config := tt.in
			err := config.Adjust()

			if tt.wantErr {
				re.ErrorContains(err, tt.errMsg)
				return
			}
			re.NoError(err) // do not check auxiliary fields
			config.v = nil
			config.lg = nil
			re.Equal(tt.want, config)
		})
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		in      *Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "default config",
			in: func() *Config {
				config, _ := NewConfig([]string{})
				return config
			}(),
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)

			err := tt.in.Validate()

			if tt.wantErr {
				re.ErrorContains(err, tt.errMsg)
				return
			}
			re.NoError(err)
		})
	}
}

func TestParseUrls(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name    string
		args    args
		want    []url.URL
		wantErr bool
	}{
		{
			name: "normal case",
			args: args{s: "http://localhost:2380,https://example.com,ftp://10.0.0.1:2380"},
			want: []url.URL{
				{
					Scheme: "http",
					Host:   "localhost:2380",
				},
				{
					Scheme: "https",
					Host:   "example.com",
				},
				{
					Scheme: "ftp",
					Host:   "10.0.0.1:2380",
				},
			},
		},
		{
			name:    "bad case",
			args:    args{s: "://localhost:2380,https://example.com,ftp://10.0.0.1:2380"},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)

			urls, err := parseUrls(tt.args.s)

			if tt.wantErr {
				re.Error(err)
				return
			}
			re.NoError(err)
			re.Equal(tt.want, urls)
		})
	}
}
