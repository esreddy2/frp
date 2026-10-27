//go:build !linux

package net

import (
	"context"
	"errors"
	"net"
	"time"
)

// ErrTransparentNotSupported indicates transparent proxying is not supported on this platform.
var ErrTransparentNotSupported = errors.New("transparent proxying is only supported on Linux")

// SetSocketTransparent is a stub on non-Linux platforms.
func SetSocketTransparent(fd uintptr, isIPv6 bool, socketMark int) error {
	return ErrTransparentNotSupported
}

// DialTCPTransparent is a stub on non-Linux platforms.
func DialTCPTransparent(
	ctx context.Context,
	localAddr *net.TCPAddr,
	dstAddr *net.TCPAddr,
	timeout time.Duration,
	socketMark int,
) (net.Conn, error) {
	return nil, ErrTransparentNotSupported
}

// DialUDPTransparent is a stub on non-Linux platforms.
func DialUDPTransparent(
	localAddr *net.UDPAddr,
	dstAddr *net.UDPAddr,
	socketMark int,
) (*net.UDPConn, error) {
	return nil, ErrTransparentNotSupported
}

// CheckTransparentSupport is a stub on non-Linux platforms.
func CheckTransparentSupport(localIP string) ([]string, error) {
	return nil, ErrTransparentNotSupported
}
