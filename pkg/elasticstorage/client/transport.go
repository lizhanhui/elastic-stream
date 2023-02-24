//nolint:unused
package client

import (
	"fmt"
	"sync"
)

// A Transport internally caches connections to servers. It is safe
// for concurrent use by multiple goroutines.
// TODO
type Transport struct {
	mu    sync.Mutex
	conns map[connKey][]*Conn // key is host:port
	keys  map[*Conn][]connKey
}

type connKey struct {
	host string
	port string
}

func (k connKey) String() string {
	return fmt.Sprintf("%s:%s", k.host, k.port)
}
