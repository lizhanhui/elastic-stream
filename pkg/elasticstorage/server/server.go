//nolint:unused
package server

import (
	"sync"
	"time"
)

// A Server is used to serve requests from the Elastic Storage
type Server struct {

	// IdleTimeout specifies how long until idle clients should be
	// closed with a GOAWAY frame. PING frames are not considered
	// activity for the purposes of IdleTimeout.
	IdleTimeout time.Duration

	mu          sync.Mutex
	activeConns map[*Conn]struct{}
}
