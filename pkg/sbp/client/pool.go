package client

import (
	"context"
	"sync"

	"github.com/AutoMQ/placement-manager/pkg/sbp/protocol"
)

type connPool struct {
	c *Client

	mu      sync.Mutex
	conns   map[address][]*conn
	dialing map[address]*dialCall // currently in-flight dials
}

func newConnPool(c *Client) *connPool {
	return &connPool{
		c:       c,
		conns:   make(map[address][]*conn),
		dialing: make(map[address]*dialCall),
	}
}

// getConn returns a connection to addr, creating one if necessary.
func (p *connPool) getConn(req protocol.OutRequest, addr address) (*conn, error) {
	for {
		p.mu.Lock()
		for _, cc := range p.conns[addr] {
			if cc.reserveNewRequest() {
				p.mu.Unlock()
				return cc, nil
			}
		}
		call := p.getStartDialLocked(req.Context(), addr)
		p.mu.Unlock()
		<-call.done
		cc, err := call.res, call.err
		if err != nil {
			return nil, err
		}
		if cc.reserveNewRequest() {
			return cc, nil
		}
	}
}

func (p *connPool) getStartDialLocked(ctx context.Context, addr string) *dialCall {
	if call, ok := p.dialing[addr]; ok {
		// A dial is already in-flight. Don't start another.
		return call
	}
	call := &dialCall{p: p, done: make(chan struct{}), ctx: ctx}
	p.dialing[addr] = call
	go call.dial(call.ctx, addr)
	return call
}

// dialCall is an in-flight Transport dial call to a host.
type dialCall struct {
	_ incomparable
	p *connPool
	// the context associated with the request that created this dialCall
	ctx  context.Context
	done chan struct{} // closed when done
	res  *conn         // valid after done is closed
	err  error         // valid after done is closed
}

// run in its own goroutine.
func (c *dialCall) dial(ctx context.Context, addr string) {
	c.res, c.err = c.p.c.dialConn(ctx, addr)

	c.p.mu.Lock()
	delete(c.p.dialing, addr)
	if c.err == nil {
		for _, v := range c.p.conns[addr] {
			if v == c.res {
				return
			}
		}
		c.p.conns[addr] = append(c.p.conns[addr], c.res)
	}
	c.p.mu.Unlock()

	close(c.done)
}

// incomparable is a zero-width, non-comparable type. Adding it to a struct
// makes that struct also non-comparable, and generally doesn't add
// any size (as long as it's first).
type incomparable [0]func()
