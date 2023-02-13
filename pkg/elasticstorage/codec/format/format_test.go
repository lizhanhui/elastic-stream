package format

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestNewFormat(t *testing.T) {
	type fields struct {
		code uint8
	}
	type wants struct {
		s    string
		code uint8
	}
	tests := []struct {
		name   string
		fields fields
		wants  wants
	}{
		{
			name:   "FlatBuffer",
			fields: fields{code: uint8(flatBuffer)},
			wants: wants{
				s:    "FlatBuffer",
				code: uint8(flatBuffer),
			},
		},
		{
			name:   "ProtoBuffer",
			fields: fields{code: uint8(protoBuffer)},
			wants: wants{
				s:    "ProtoBuffer",
				code: uint8(protoBuffer),
			},
		},
		{
			name:   "JSON",
			fields: fields{code: uint8(json)},
			wants: wants{
				s:    "JSON",
				code: uint8(json),
			},
		},
		{
			name:   "Unknown",
			fields: fields{code: uint8(unknown)},
			wants: wants{
				s:    "Unknown",
				code: uint8(unknown),
			},
		},
		{
			name:   "Unknown code",
			fields: fields{code: 42},
			wants: wants{
				s:    "Unknown",
				code: uint8(unknown),
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)

			f := NewFormat(tt.fields.code)

			re.Equal(tt.wants.s, f.String())
			re.Equal(tt.wants.code, f.Code())
		})
	}
}

func TestFormat(t *testing.T) {
	tests := []struct {
		name string
		f    func() Format
		want Format
	}{
		{
			name: "FlatBuffer",
			f:    FlatBufferEnum,
			want: NewFormat(uint8(flatBuffer)),
		},
		{
			name: "ProtoBuffer",
			f:    ProtoBufferEnum,
			want: NewFormat(uint8(protoBuffer)),
		},
		{
			name: "JSON",
			f:    JSONEnum,
			want: NewFormat(uint8(json)),
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)

			re.Equal(tt.want, tt.f())
		})
	}
}
