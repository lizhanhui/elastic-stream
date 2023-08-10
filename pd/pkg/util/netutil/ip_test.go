package netutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetOutboundIP(t *testing.T) {
	ip, err := GetOutboundIP()
	require.NoError(t, err)
	require.NotNil(t, ip)
}

func TestGetNonLoopbackIP(t *testing.T) {
	ip, err := GetNonLoopbackIP()
	require.NoError(t, err)
	require.NotNil(t, ip)
	t.Logf("non-loopback IP: %s", ip)
}
