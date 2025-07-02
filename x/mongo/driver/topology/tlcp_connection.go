package topology

import (
	"context"
	"net"

	"gitee.com/Trisia/gotlcp/tlcp"
)

type tlcpConn interface {
	net.Conn

	// Require HandshakeContext on the interface for Go 1.17 and higher.
	HandshakeContext(ctx context.Context) error
	ConnectionState() tlcp.ConnectionState
}

var _ tlcpConn = (*tlcp.Conn)(nil)

type tlcpConnectionSource interface {
	Client(net.Conn, *tlcp.Config) tlcpConn
}

type tlcpConnectionSourceFn func(net.Conn, *tlcp.Config) tlcpConn

var _ tlcpConnectionSource = (tlcpConnectionSourceFn)(nil)

func (t tlcpConnectionSourceFn) Client(nc net.Conn, cfg *tlcp.Config) tlcpConn {
	return t(nc, cfg)
}

var defaultTLCPConnectionSource tlcpConnectionSourceFn = func(nc net.Conn, cfg *tlcp.Config) tlcpConn {
	return tlcp.Client(nc, cfg)
}

// clientHandshake will perform a handshake on Go 1.17 and higher with HandshakeContext.
func clientTLCPHandshake(ctx context.Context, client tlcpConn) error {
	return client.HandshakeContext(ctx)
}
