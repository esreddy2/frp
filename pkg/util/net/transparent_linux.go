//go:build linux

package net

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// ErrTransparentNotSupported indicates transparent proxying is not supported on this platform.
var ErrTransparentNotSupported = errors.New("transparent proxying is only supported on Linux")

// SetSocketTransparent applies SO_REUSEADDR, SO_REUSEPORT, IP_TRANSPARENT (or IPV6_TRANSPARENT),
// and optionally SO_MARK to a raw socket file descriptor.
func SetSocketTransparent(fd uintptr, isIPv6 bool, socketMark int) error {
	// Enable SO_REUSEADDR and SO_REUSEPORT
	if err := unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
		return fmt.Errorf("setsockopt SO_REUSEADDR: %w", err)
	}
	if err := unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1); err != nil {
		return fmt.Errorf("setsockopt SO_REUSEPORT: %w", err)
	}

	// Apply IP_TRANSPARENT
	if isIPv6 {
		if err := unix.SetsockoptInt(int(fd), unix.SOL_IPV6, unix.IPV6_TRANSPARENT, 1); err != nil {
			return fmt.Errorf("setsockopt IPV6_TRANSPARENT (requires CAP_NET_ADMIN): %w", err)
		}
	} else {
		if err := unix.SetsockoptInt(int(fd), unix.SOL_IP, unix.IP_TRANSPARENT, 1); err != nil {
			return fmt.Errorf("setsockopt IP_TRANSPARENT (requires CAP_NET_ADMIN): %w", err)
		}
	}

	// Apply SO_MARK if requested
	if socketMark > 0 {
		if err := unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_MARK, socketMark); err != nil {
			return fmt.Errorf("setsockopt SO_MARK (%d): %w", socketMark, err)
		}
	}
	return nil
}

// DialTCPTransparent dials a TCP connection to dstAddr using localAddr as the spoofed source address.
// It sets IP_TRANSPARENT and SO_REUSEADDR/SO_REUSEPORT before binding.
// If dialing with localAddr fails due to port collision (EADDRINUSE), it automatically retries with localAddr.IP:0
// to preserve the client IP with an ephemeral source port.
func DialTCPTransparent(
	ctx context.Context,
	localAddr *net.TCPAddr,
	dstAddr *net.TCPAddr,
	timeout time.Duration,
	socketMark int,
) (net.Conn, error) {
	if dstAddr == nil {
		return nil, errors.New("destination address cannot be nil")
	}

	dialer := &net.Dialer{
		LocalAddr: localAddr,
		Timeout:   timeout,
		Control: func(network, address string, c syscall.RawConn) error {
			var ctrlErr error
			err := c.Control(func(fd uintptr) {
				isIPv6 := localAddr != nil && localAddr.IP != nil && localAddr.IP.To4() == nil
				ctrlErr = SetSocketTransparent(fd, isIPv6, socketMark)
			})
			if err != nil {
				return err
			}
			return ctrlErr
		},
	}

	conn, err := dialer.DialContext(ctx, "tcp", dstAddr.String())
	if err != nil && localAddr != nil && localAddr.Port != 0 {
		// Fallback to ephemeral port on port collision
		fallbackAddr := &net.TCPAddr{
			IP:   localAddr.IP,
			Port: 0,
			Zone: localAddr.Zone,
		}
		dialer.LocalAddr = fallbackAddr
		conn, err = dialer.DialContext(ctx, "tcp", dstAddr.String())
	}
	return conn, err
}

// DialUDPTransparent dials a UDP connection to dstAddr using localAddr as the spoofed source address.
// It sets IP_TRANSPARENT and SO_REUSEADDR/SO_REUSEPORT before binding.
// If dialing with localAddr fails due to port collision (EADDRINUSE), it automatically retries with localAddr.IP:0.
func DialUDPTransparent(
	localAddr *net.UDPAddr,
	dstAddr *net.UDPAddr,
	socketMark int,
) (*net.UDPConn, error) {
	if dstAddr == nil {
		return nil, errors.New("destination address cannot be nil")
	}

	dialer := &net.Dialer{
		LocalAddr: localAddr,
		Control: func(network, address string, c syscall.RawConn) error {
			var ctrlErr error
			err := c.Control(func(fd uintptr) {
				isIPv6 := localAddr != nil && localAddr.IP != nil && localAddr.IP.To4() == nil
				ctrlErr = SetSocketTransparent(fd, isIPv6, socketMark)
			})
			if err != nil {
				return err
			}
			return ctrlErr
		},
	}

	c, err := dialer.Dial("udp", dstAddr.String())
	if err != nil && localAddr != nil && localAddr.Port != 0 {
		fallbackAddr := &net.UDPAddr{
			IP:   localAddr.IP,
			Port: 0,
			Zone: localAddr.Zone,
		}
		dialer.LocalAddr = fallbackAddr
		c, err = dialer.Dial("udp", dstAddr.String())
	}
	if err != nil {
		return nil, err
	}

	udpConn, ok := c.(*net.UDPConn)
	if !ok {
		_ = c.Close()
		return nil, fmt.Errorf("dialed conn is not *net.UDPConn: %T", c)
	}
	return udpConn, nil
}

// CheckTransparentSupport checks whether the system supports transparent proxying
// and verifies relevant kernel networking configuration (sysctl) on Linux.
func CheckTransparentSupport(localIP string) ([]string, error) {
	var warnings []string

	// 1. Check capability by testing IP_TRANSPARENT setsockopt on a dummy socket
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err == nil {
		defer unix.Close(fd)
		if optErr := unix.SetsockoptInt(fd, unix.SOL_IP, unix.IP_TRANSPARENT, 1); optErr != nil {
			if errors.Is(optErr, syscall.EPERM) {
				return nil, fmt.Errorf("transparent proxying requires CAP_NET_ADMIN capability: %w", optErr)
			}
		}
	}

	// 2. Check sysctl policies: route_localnet
	parsedIP := net.ParseIP(localIP)
	if parsedIP != nil && parsedIP.IsLoopback() {
		if val, readErr := readSysctl("/proc/sys/net/ipv4/conf/all/route_localnet"); readErr == nil && val == "0" {
			warnings = append(warnings, fmt.Sprintf(
				"localIP [%s] is loopback but /proc/sys/net/ipv4/conf/all/route_localnet is disabled (0); "+
					"kernel will drop non-local spoofed packets on loopback unless "+
					"'sysctl -w net.ipv4.conf.all.route_localnet=1' is set", localIP))
		}
	}

	// 3. Check sysctl policies: ip_forward
	if val, readErr := readSysctl("/proc/sys/net/ipv4/ip_forward"); readErr == nil && val == "0" {
		warnings = append(warnings,
			"/proc/sys/net/ipv4/ip_forward is disabled (0); "+
				"transparent policy routing across interfaces may fail unless 'sysctl -w net.ipv4.ip_forward=1' is set")
	}

	return warnings, nil
}

func readSysctl(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}
