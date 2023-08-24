package netutil

import (
	"net"

	"github.com/pkg/errors"
)

// GetOutboundIP returns the preferred outbound ip of this machine
func GetOutboundIP() (net.IP, error) {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return nil, err
	}
	defer func(conn net.Conn) { _ = conn.Close() }(conn)

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP, nil
}

// GetNonLoopbackIP returns the first non-loopback IP of this machine
func GetNonLoopbackIP() (net.IP, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}

	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			ip, _, err := net.ParseCIDR(addr.String())
			if err != nil {
				continue
			}
			if ip.To4() != nil {
				return ip, nil
			}
		}
	}

	return nil, errors.New("no non-loopback IP address found")
}
