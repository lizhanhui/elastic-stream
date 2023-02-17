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
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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
				Log: func() *Log {
					log := NewLog()
					log.Level = "INFO"
					log.Rotate.MaxSize = 64
					log.Rotate.MaxAge = 180
					return log
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
			name: "default config in toml",
			args: args{arguments: []string{
				"--config=../../../conf/config.toml",
			}},
			want: Config{
				Etcd: func() *embed.Config {
					config := embed.NewConfig()
					config.InitialClusterToken = "pm-cluster"
					return config
				}(),
				Log: func() *Log {
					log := NewLog()
					log.Level = "INFO"
					log.Rotate.MaxSize = 64
					log.Rotate.MaxAge = 180
					return log
				}(),
				PeerUrls:                    "http://127.0.0.1:2380",
				ClientUrls:                  "http://127.0.0.1:2379",
				AdvertisePeerUrls:           "http://127.0.0.1:2380",
				AdvertiseClientUrls:         "http://127.0.0.1:2379",
				Name:                        "pm-hostname",
				DataDir:                     "default.pm-hostname",
				InitialCluster:              "pm=http://127.0.0.1:2380",
				LeaderLease:                 3,
				LeaderPriorityCheckInterval: time.Minute,
			},
		},
		{
			name: "default config in yaml",
			args: args{arguments: []string{
				"--config=../../../conf/config.yaml",
			}},
			want: Config{
				Etcd: func() *embed.Config {
					config := embed.NewConfig()
					config.InitialClusterToken = "pm-cluster"
					return config
				}(),
				Log: func() *Log {
					log := NewLog()
					log.Level = "INFO"
					log.Rotate.MaxSize = 64
					log.Rotate.MaxAge = 180
					return log
				}(),
				PeerUrls:                    "http://127.0.0.1:2380",
				ClientUrls:                  "http://127.0.0.1:2379",
				AdvertisePeerUrls:           "http://127.0.0.1:2380",
				AdvertiseClientUrls:         "http://127.0.0.1:2379",
				Name:                        "pm-hostname",
				DataDir:                     "default.pm-hostname",
				InitialCluster:              "pm=http://127.0.0.1:2380",
				LeaderLease:                 3,
				LeaderPriorityCheckInterval: time.Minute,
			},
		},
		{
			name: "config from command line (override config in file)",
			args: args{arguments: []string{
				"--config=../../../conf/config.toml",

				"--peer-urls=test-peer-urls",
				"--client-urls=test-client-urls",
				"--advertise-peer-urls=test-advertise-peer-urls",
				"--advertise-client-urls=test-advertise-client-urls",
				"--name=test-name",
				"--data-dir=test-data-dir",
				"--initial-cluster=test-initial-cluster",
				"--leader-lease=123",
				"--leader-priority-check-interval=1h1m1s",
				"--etcd-initial-cluster-token=test-initial-cluster-token",
				"--log-level=FATAL",
				"--log-zap-encoding=console",
				"--log-zap-output-paths=stdout,stderr",
				"--log-zap-error-output-paths=stdout,stderr",
				"--log-enable-rotation=false",
				"--log-rotate-max-size=1234",
				"--log-rotate-max-age=12345",
				"--log-rotate-max-backups=123456",
				"--log-rotate-local-time=true",
				"--log-rotate-compress=true",
			}},
			want: Config{
				Etcd: func() *embed.Config {
					config := embed.NewConfig()
					config.InitialClusterToken = "test-initial-cluster-token"
					return config
				}(),
				Log: func() *Log {
					log := NewLog()
					log.Level = "FATAL"
					log.Zap.Level = zap.NewAtomicLevelAt(zapcore.FatalLevel)
					log.Zap.Encoding = "console"
					log.Zap.OutputPaths = []string{"stdout", "stderr"}
					log.Zap.ErrorOutputPaths = []string{"stdout", "stderr"}
					log.EnableRotation = false
					log.Rotate.MaxSize = 1234
					log.Rotate.MaxAge = 12345
					log.Rotate.MaxBackups = 123456
					log.Rotate.LocalTime = true
					log.Rotate.Compress = true
					return log
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
					config.TickMs = 123
					config.ElectionMs = 1234
					return config
				}(),
				Log: func() *Log {
					log := NewLog()
					log.Level = "FATAL"
					log.Zap.Level = zap.NewAtomicLevelAt(zapcore.FatalLevel)
					log.Zap.Encoding = "console"
					log.Zap.OutputPaths = []string{"stdout", "stderr"}
					log.Zap.ErrorOutputPaths = []string{"stdout", "stderr"}
					log.Zap.DisableCaller = true
					log.Zap.DisableStacktrace = true
					log.Zap.EncoderConfig.MessageKey = "test-msg"
					log.EnableRotation = false
					log.Rotate.MaxSize = 1234
					log.Rotate.MaxAge = 12345
					log.Rotate.MaxBackups = 123456
					log.Rotate.LocalTime = true
					log.Rotate.Compress = true
					return log
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
					config.TickMs = 123
					config.ElectionMs = 1234
					return config
				}(),
				Log: func() *Log {
					log := NewLog()
					log.Level = "FATAL"
					log.Zap.Level = zap.NewAtomicLevelAt(zapcore.FatalLevel)
					log.Zap.Encoding = "console"
					log.Zap.OutputPaths = []string{"stdout", "stderr"}
					log.Zap.ErrorOutputPaths = []string{"stdout", "stderr"}
					log.Zap.DisableCaller = true
					log.Zap.DisableStacktrace = true
					log.Zap.EncoderConfig.MessageKey = "test-msg"
					log.EnableRotation = false
					log.Rotate.MaxSize = 1234
					log.Rotate.MaxAge = 12345
					log.Rotate.MaxBackups = 123456
					log.Rotate.LocalTime = true
					log.Rotate.Compress = true
					return log
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
		{
			name: "adjust log config error",
			args: args{arguments: []string{
				"--log-level=LEVEL",
			}},
			wantErr: true,
			errMsg:  "adjust log config",
		},
		{
			name: "create logger error",
			args: args{arguments: []string{
				"--log-zap-encoding=raw",
			}},
			wantErr: true,
			errMsg:  "build logger",
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

			equal(re, tt.want.Log.Zap, config.Log.Zap)
			tt.want.Log.Zap = zap.Config{}
			config.Log.Zap = zap.Config{}

			re.Equal(tt.want, *config)
		})
	}
}

func TestConfig_Adjust(t *testing.T) {
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
				Log: func() *Log {
					log := NewLog()
					log.Level = "INFO"
					log.Rotate.MaxSize = 64
					log.Rotate.MaxAge = 180
					return log
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
				Log: func() *Log {
					log := NewLog()
					log.Level = "INFO"
					log.Rotate.MaxSize = 64
					log.Rotate.MaxAge = 180
					return log
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
			name: "invalid advertise client url",
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
			re.NoError(err)

			// do not check auxiliary fields
			config.v = nil
			config.lg = nil

			equal(re, tt.want.Log.Zap, config.Log.Zap)
			tt.want.Log.Zap = zap.Config{}
			config.Log.Zap = zap.Config{}

			re.Equal(tt.want, config)
		})
	}
}

func equal(re *require.Assertions, wantZap zap.Config, actualZap zap.Config) {
	re.Equal(wantZap.Level.String(), actualZap.Level.String())
	re.Equal(wantZap.Encoding, actualZap.Encoding)
	re.Equal(wantZap.OutputPaths, actualZap.OutputPaths)
	re.Equal(wantZap.ErrorOutputPaths, actualZap.ErrorOutputPaths)
	re.Equal(wantZap.Development, actualZap.Development)
	re.Equal(wantZap.DisableStacktrace, actualZap.DisableStacktrace)
	re.Equal(wantZap.DisableCaller, actualZap.DisableCaller)
}

func TestConfig_Validate(t *testing.T) {
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
