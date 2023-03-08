package protocol

import (
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/require"

	"github.com/AutoMQ/placement-manager/pkg/sbp/codec/format"
	"github.com/AutoMQ/placement-manager/pkg/util/fbutil"
)

func TestListRangesResponse_Marshal(t *testing.T) {
	var mockListRangesResponse ListRangesResponse
	_ = gofakeit.Struct(&mockListRangesResponse)
	mockData := fbutil.Marshal(mockListRangesResponse.ListRangesResponseT)
	tests := []struct {
		name    string
		resp    ListRangesResponse
		fmt     format.Format
		want    []byte
		wantErr bool
		errMsg  string
	}{
		{
			name: "FlatBuffer",
			resp: mockListRangesResponse,
			fmt:  format.FlatBuffer(),
			want: mockData,
		},
		{
			name:    "ProtoBuffer",
			resp:    mockListRangesResponse,
			fmt:     format.ProtoBuffer(),
			wantErr: true,
			errMsg:  "unsupported response format",
		},
		{
			name:    "JSON",
			resp:    mockListRangesResponse,
			fmt:     format.JSON(),
			wantErr: true,
			errMsg:  "unsupported response format",
		},
		{
			name:    "Unknown",
			resp:    mockListRangesResponse,
			fmt:     format.NewFormat(0),
			wantErr: true,
			errMsg:  "unsupported response format",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)

			got, err := tt.resp.Marshal(tt.fmt)
			if tt.wantErr {
				re.ErrorContains(err, tt.errMsg)
			} else {
				re.NoError(err)
				re.Equal(tt.want, got)
			}
		})
	}
}
