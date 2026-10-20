package naming

import "strings"

// AddUserPrefix builds the wire-level proxy name for frps by prefixing user.
func AddUserPrefix(user, name string) string {
	if user == "" {
		return name
	}
	return user + "." + name
}

// StripUserPrefix converts a wire-level proxy name to an internal raw name.
// It strips only one exact "{user}." prefix.
func StripUserPrefix(user, name string) string {
	if user == "" {
		return name
	}
	if trimmed, ok := strings.CutPrefix(name, user+"."); ok {
		return trimmed
	}
	return name
}

// MaxDatagramIdentifierLength is the maximum number of bytes of a
// wire-level proxy name that fit in the identifier field of a QUIC datagram
// frame. The field holds a single length byte, so a longer wire-level name
// cannot be addressed by the datagram lane at all.
const MaxDatagramIdentifierLength = 255

// DatagramIdentifierFits reports whether a wire-level proxy name fits in
// the identifier field of a QUIC datagram frame.
func DatagramIdentifierFits(wireName string) bool {
	return len(wireName) <= MaxDatagramIdentifierLength
}

// BuildTargetServerProxyName resolves visitor target proxy name for wire-level
// protocol messages. serverUser overrides local user when set.
func BuildTargetServerProxyName(localUser, serverUser, serverName string) string {
	if serverUser != "" {
		return AddUserPrefix(serverUser, serverName)
	}
	return AddUserPrefix(localUser, serverName)
}
