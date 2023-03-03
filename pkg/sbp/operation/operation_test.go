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
			fields: fields{code: ping},
			wants: wants{
				s:    "Ping",
				code: ping,
			},
		},
		{
			name:   "GoAway",
			fields: fields{code: goAway},
			wants: wants{
				s:    "GoAway",
				code: goAway,
			},
		},
		{
			name:   "Publish",
			fields: fields{code: publish},
			wants: wants{
				s:    "Publish",
				code: publish,
			},
		},
		{
			name:   "Heartbeat",
			fields: fields{code: heartbeat},
			wants: wants{
				s:    "Heartbeat",
				code: heartbeat,
			},
		},
		{
			name:   "ListRange",
			fields: fields{code: listRange},
			wants: wants{
				s:    "ListRange",
				code: listRange,
			},
		},
		{
			name:   "Unknown",
			fields: fields{code: unknown},
			wants: wants{
				s:    "Unknown",
				code: unknown,
			},
		},
		{
			name:   "Unknown code",
			fields: fields{code: 42},
			wants: wants{
				s:    "Unknown",
				code: unknown,
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

func TestIsControl(t *testing.T) {
	t.Parallel()
	re := require.New(t)

	for _, operation := range []Operation{Ping(), GoAway(), Heartbeat()} {
		re.True(operation.IsControl())
	}
	for _, operation := range []Operation{Publish(), ListRange()} {
		re.False(operation.IsControl())
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
			want:   NewOperation(ping),
		},
		{
			name:   "GoAway",
			opFunc: GoAway,
			want:   NewOperation(goAway),
		},
		{
			name:   "Publish",
			opFunc: Publish,
			want:   NewOperation(publish),
		},
		{
			name:   "Heartbeat",
			opFunc: Heartbeat,
			want:   NewOperation(heartbeat),
		},
		{
			name:   "ListRange",
			opFunc: ListRange,
			want:   NewOperation(listRange),
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
