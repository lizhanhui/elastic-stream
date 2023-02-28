//nolint:unused
package client

import (
	"time"
)

// Client is an SBP client
// TODO
type Client struct {
	Transport Transport
	Timeout   time.Duration
}
