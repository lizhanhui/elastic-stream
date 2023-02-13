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
	type wants struct {
		s    string
		code uint16
	}
	tests := []struct {
		name   string
		fields fields
		wants  wants
	}{
		{
			name:   "Ping",
			fields: fields{code: uint16(ping)},
			wants: wants{
				s:    "Ping",
				code: uint16(ping),
			},
		},
		{
			name:   "GoAway",
			fields: fields{code: uint16(goAway)},
			wants: wants{
				s:    "GoAway",
				code: uint16(goAway),
			},
		},
		{
			name:   "Publish",
			fields: fields{code: uint16(publish)},
			wants: wants{
				s:    "Publish",
				code: uint16(publish),
			},
		},
		{
			name:   "Heartbeat",
			fields: fields{code: uint16(heartbeat)},
			wants: wants{
				s:    "Heartbeat",
				code: uint16(heartbeat),
			},
		},
		{
			name:   "ListRange",
			fields: fields{code: uint16(listRange)},
			wants: wants{
				s:    "ListRange",
				code: uint16(listRange),
			},
		},
		{
			name:   "Unknown",
			fields: fields{code: uint16(unknown)},
			wants: wants{
				s:    "Unknown",
				code: uint16(unknown),
			},
		},
		{
			name:   "Unknown code",
			fields: fields{code: 42},
			wants: wants{
				s:    "Unknown",
				code: uint16(unknown),
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)

			o := NewOperation(tt.fields.code)

			re.Equal(tt.wants.s, o.String())
			re.Equal(tt.wants.code, o.Code())
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
