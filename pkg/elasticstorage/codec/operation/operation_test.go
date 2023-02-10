package operation

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestNewOperation(t *testing.T) {
	type fields struct {
		code uint16
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name:   "Ping",
			fields: fields{code: uint16(ping)},
			want:   "Ping",
		},
		{
			name:   "GoAway",
			fields: fields{code: uint16(goAway)},
			want:   "GoAway",
		},
		{
			name:   "Publish",
			fields: fields{code: uint16(publish)},
			want:   "Publish",
		},
		{
			name:   "Heartbeat",
			fields: fields{code: uint16(heartbeat)},
			want:   "Heartbeat",
		},
		{
			name:   "ListRange",
			fields: fields{code: uint16(listRange)},
			want:   "ListRange",
		},
		{
			name:   "Unknown",
			fields: fields{code: uint16(unknown)},
			want:   "Unknown",
		},
		{
			name:   "Unknown code",
			fields: fields{code: 42},
			want:   "Unknown",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)

			o := NewOperation(tt.fields.code)

			re.Equal(tt.want, o.String())
		})
	}
}

func TestOperation(t *testing.T) {
	tests := []struct {
		name   string
		opFunc func() Operation
		want   Operation
	}{
		{
			name:   "Ping",
			opFunc: Ping,
			want:   NewOperation(uint16(ping)),
		},
		{
			name:   "GoAway",
			opFunc: GoAway,
			want:   NewOperation(uint16(goAway)),
		},
		{
			name:   "Publish",
			opFunc: Publish,
			want:   NewOperation(uint16(publish)),
		},
		{
			name:   "Heartbeat",
			opFunc: Heartbeat,
			want:   NewOperation(uint16(heartbeat)),
		},
		{
			name:   "ListRange",
			opFunc: ListRange,
			want:   NewOperation(uint16(listRange)),
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)

			re.Equal(tt.want, tt.opFunc())
		})
	}
}
