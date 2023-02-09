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
		code Code
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name:   "FlatBuffer",
			fields: fields{code: flatBuffer},
			want:   "FlatBuffer",
		},
		{
			name:   "ProtoBuffer",
			fields: fields{code: protoBuffer},
			want:   "ProtoBuffer",
		},
		{
			name:   "JSON",
			fields: fields{code: json},
			want:   "JSON",
		},
		{
			name:   "Unknown",
			fields: fields{code: unknown},
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

			f := NewFormat(tt.fields.code)

			re.Equal(tt.want, f.String())
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
			want: NewFormat(flatBuffer),
		},
		{
			name: "ProtoBuffer",
			f:    ProtoBufferEnum,
			want: NewFormat(protoBuffer),
		},
		{
			name: "JSON",
			f:    JSONEnum,
			want: NewFormat(json),
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
