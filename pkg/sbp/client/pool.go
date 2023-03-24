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
	addrs   map[*conn][]address
	dialing map[address]*dialCall // currently in-flight dials
}

func newConnPool(c *Client) *connPool {
	return &connPool{
		c:       c,
		conns:   make(map[address][]*conn),
		addrs:   make(map[*conn][]address),
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

func (p *connPool) markDead(cc *conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, addr := range p.addrs[cc] {
		vv, ok := p.conns[addr]
		if !ok {
			continue
		}
		newList := filterOutConn(vv, cc)
		if len(newList) > 0 {
			p.conns[addr] = newList
		} else {
			delete(p.conns, addr)
		}
	}
	delete(p.addrs, cc)
}

func filterOutConn(in []*conn, exclude *conn) []*conn {
	out := in[:0]
	for _, v := range in {
		if v != exclude {
			out = append(out, v)
		}
	}
	// If we filtered it out, zero out the last item to prevent
	// the GC from seeing it.
	if len(in) != len(out) {
		in[len(in)-1] = nil
	}
	return out
}

// dialCall is an in-flight Transport dial call to a host.
type dialCall struct {
	p *connPool
	// the context associated with the request that created this dialCall
	ctx  context.Context
	done chan struct{} // closed when done
	res  *conn         // valid after done is closed
	err  error         // valid after done is closed
}

// run in its own goroutine.
func (c *dialCall) dial(ctx context.Context, addr string) {
	cc, err := c.p.c.dialConn(ctx, addr)
	c.res = cc
	c.err = err

	c.p.mu.Lock()
	delete(c.p.dialing, addr)
	if err == nil {
		for _, v := range c.p.conns[addr] {
			if v == cc {
				return
			}
		}
		c.p.conns[addr] = append(c.p.conns[addr], cc)
		c.p.addrs[cc] = append(c.p.addrs[cc], addr)
	}
	c.p.mu.Unlock()

	close(c.done)
}
