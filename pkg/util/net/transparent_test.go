package net

import (
	"context"
	"errors"
	"net"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDialTransparentNilDestination(t *testing.T) {
	_, err := DialTCPTransparent(context.Background(), nil, nil, time.Second, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "destination address cannot be nil")

	_, err = DialUDPTransparent(nil, nil, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "destination address cannot be nil")
}

func TestDialTCPTransparentPermissionOrSupport(t *testing.T) {
	if runtime.GOOS != "linux" {
		local := &net.TCPAddr{IP: net.IPv4(1, 2, 3, 4), Port: 1234}
		dst := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 80}
		_, err := DialTCPTransparent(context.Background(), local, dst, time.Second, 0)
		require.ErrorIs(t, err, ErrTransparentNotSupported)
		return
	}

	// On Linux without CAP_NET_ADMIN, setsockopt(IP_TRANSPARENT) returns EPERM
	localAddr := &net.TCPAddr{IP: net.IPv4(198, 51, 100, 1), Port: 54321}
	dstAddr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 59999}

	_, err := DialTCPTransparent(context.Background(), localAddr, dstAddr, time.Second, 0)
	require.Error(t, err)
	if errors.Is(err, syscall.EPERM) {
		require.Contains(t, err.Error(), "CAP_NET_ADMIN")
	}
}

func TestDialUDPTransparentPermissionOrSupport(t *testing.T) {
	if runtime.GOOS != "linux" {
		local := &net.UDPAddr{IP: net.IPv4(1, 2, 3, 4), Port: 1234}
		dst := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 80}
		_, err := DialUDPTransparent(local, dst, 0)
		require.ErrorIs(t, err, ErrTransparentNotSupported)
		return
	}

	localAddr := &net.UDPAddr{IP: net.IPv4(198, 51, 100, 1), Port: 54321}
	dstAddr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 59999}

	_, err := DialUDPTransparent(localAddr, dstAddr, 0)
	require.Error(t, err)
	if errors.Is(err, syscall.EPERM) {
		require.Contains(t, err.Error(), "CAP_NET_ADMIN")
	}
}

func TestCheckTransparentSupport(t *testing.T) {
	warnings, err := CheckTransparentSupport("127.0.0.1")
	if runtime.GOOS != "linux" {
		require.ErrorIs(t, err, ErrTransparentNotSupported)
		return
	}
	// On Linux without CAP_NET_ADMIN, err should indicate missing capability
	if err != nil {
		require.Contains(t, err.Error(), "CAP_NET_ADMIN")
	}
	_ = warnings
}
