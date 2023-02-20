package client

import (
	"time"
)

// A Client is used to communicate with the Elastic Storage
// TODO
type Client struct {
	Transport Transport
	Timeout   time.Duration
}
