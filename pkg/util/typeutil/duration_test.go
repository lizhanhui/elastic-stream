package typeutil

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"
)

type duration struct {
	Duration Duration `json:"duration" toml:"duration"`
}

func TestDuration_MarshalJSON(t *testing.T) {
	type fields struct {
		Duration time.Duration
	}
	tests := []struct {
		name    string
		fields  fields
		want    []byte
		wantErr bool
	}{
		{
			name:    "zero case",
			fields:  fields{Duration: time.Duration(0)},
			want:    []byte(`{"duration":"0s"}`),
			wantErr: false,
		},
		{
			name:    "normal case",
			fields:  fields{Duration: time.Hour + time.Minute + time.Second + time.Millisecond + time.Microsecond},
			want:    []byte(`{"duration":"1h1m1.001001s"}`),
			wantErr: false,
		},
		{
			name:    "negative case",
			fields:  fields{Duration: -time.Hour - time.Minute - time.Second - time.Millisecond - time.Microsecond},
			want:    []byte(`{"duration":"-1h1m1.001001s"}`),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)

			d := &duration{
				Duration: NewDuration(tt.fields.Duration),
			}
			got, err := json.Marshal(d)

			if tt.wantErr {
				re.Error(err)
			} else {
				re.NoError(err)
				re.Equal(tt.want, got)
			}
		})
	}
}

func TestDuration_MarshalText(t *testing.T) {
	type fields struct {
		Duration time.Duration
	}
	tests := []struct {
		name    string
		fields  fields
		want    []byte
		wantErr bool
	}{
		{
			name:    "zero case",
			fields:  fields{Duration: time.Duration(0)},
			want:    []byte("[duration]\n  Duration = 0\n"),
			wantErr: false,
		},
		{
			name:    "normal case",
			fields:  fields{Duration: time.Hour + time.Minute + time.Second + time.Millisecond + time.Microsecond},
			want:    []byte("[duration]\n  Duration = 3661001001000\n"),
			wantErr: false,
		},
		{
			name:    "negative case",
			fields:  fields{Duration: -time.Hour - time.Minute - time.Second - time.Millisecond - time.Microsecond},
			want:    []byte("[duration]\n  Duration = -3661001001000\n"),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)

			d := &duration{
				Duration: NewDuration(tt.fields.Duration),
			}
			var b bytes.Buffer
			err := toml.NewEncoder(&b).Encode(d)
			got := b.Bytes()

			if tt.wantErr {
				re.Error(err)
			} else {
				re.NoError(err)
				re.Equal(tt.want, got)
			}
		})
	}
}

func TestDuration_UnmarshalJSON(t *testing.T) {
	type args struct {
		text []byte
	}
	tests := []struct {
		name    string
		args    args
		want    duration
		wantErr bool
	}{
		{
			name:    "(number) zero case",
			args:    args{text: []byte(`{"duration":0}`)},
			want:    duration{Duration: NewDuration(time.Duration(0))},
			wantErr: false,
		},
		{
			name:    "(string) zero case",
			args:    args{text: []byte(`{"duration":"0s"}`)},
			want:    duration{Duration: NewDuration(time.Duration(0))},
			wantErr: false,
		},
		{
			name:    "(number) normal case",
			args:    args{text: []byte(`{"duration":3661001001000}`)},
			want:    duration{Duration: NewDuration(time.Hour + time.Minute + time.Second + time.Millisecond + time.Microsecond)},
			wantErr: false,
		},
		{
			name:    "(string) normal case",
			args:    args{text: []byte(`{"duration":"1h1m1.001001s"}`)},
			want:    duration{Duration: NewDuration(time.Hour + time.Minute + time.Second + time.Millisecond + time.Microsecond)},
			wantErr: false,
		},
		{
			name:    "(number) negative case",
			args:    args{text: []byte(`{"duration":-3661001001000}`)},
			want:    duration{Duration: NewDuration(-time.Hour - time.Minute - time.Second - time.Millisecond - time.Microsecond)},
			wantErr: false,
		},
		{
			name:    "(string) negative case",
			args:    args{text: []byte(`{"duration":"-1h1m1.001001s"}`)},
			want:    duration{Duration: NewDuration(-time.Hour - time.Minute - time.Second - time.Millisecond - time.Microsecond)},
			wantErr: false,
		},
		{
			name:    "(string) bad case",
			args:    args{text: []byte(`{"duration":"0ks"}`)},
			wantErr: true,
		},
		{
			name:    "wrong type",
			args:    args{text: []byte(`{"duration":false}`)},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)

			got := &duration{}
			err := json.Unmarshal(tt.args.text, got)

			if tt.wantErr {
				re.Error(err)
			} else {
				re.NoError(err)
				re.Equal(tt.want, *got)
			}
		})
	}
}

func TestDuration_UnmarshalText(t *testing.T) {
	type args struct {
		text []byte
	}
	tests := []struct {
		name    string
		args    args
		want    duration
		wantErr bool
	}{
		{
			name:    "zero case",
			args:    args{text: []byte(`duration = "0s"`)},
			want:    duration{Duration: NewDuration(time.Duration(0))},
			wantErr: false,
		},
		{
			name:    "normal case",
			args:    args{text: []byte(`duration = "1h1m1.001001s"`)},
			want:    duration{Duration: NewDuration(time.Hour + time.Minute + time.Second + time.Millisecond + time.Microsecond)},
			wantErr: false,
		},
		{
			name:    "negative case",
			args:    args{text: []byte(`duration = "-1h1m1.001001s"`)},
			want:    duration{Duration: NewDuration(-time.Hour - time.Minute - time.Second - time.Millisecond - time.Microsecond)},
			wantErr: false,
		},
		{
			name:    "bad case",
			args:    args{text: []byte(`"0s"`)},
			wantErr: true,
		},
		{
			name:    "wrong type",
			args:    args{text: []byte(`duration = "0ks"`)},
			wantErr: true,
		},
		{
			name:    "wrong name",
			args:    args{text: []byte(`ration = "1s"`)},
			want:    duration{Duration: NewDuration(time.Duration(0))},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)

			got := &duration{}
			err := toml.Unmarshal(tt.args.text, got)

			if tt.wantErr {
				re.Error(err)
			} else {
				re.NoError(err)
				re.Equal(tt.want, *got)
			}
		})
	}
}
