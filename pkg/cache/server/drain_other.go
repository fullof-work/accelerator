//go:build !linux

package server

import (
	"context"
	"net"
)

// Non-Linux builds retain the ordinary net.Conn close path. The Linux service
// deployment additionally fences unread-input TCP reset using TIOCOUTQ.
func finishResponseDelivery(context.Context, net.Conn) error { return nil }
