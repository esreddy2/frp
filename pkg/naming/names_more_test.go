package naming

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAddUserPrefixMore(t *testing.T) {
	require := require.New(t)

	// Empty user leaves name unchanged.
	require.Equal("test", AddUserPrefix("", "test"))
	require.Equal("", AddUserPrefix("", ""))

	// Non-empty user prefixes with a dot separator.
	require.Equal("alice.test", AddUserPrefix("alice", "test"))
	require.Equal("alice.", AddUserPrefix("alice", ""))

	// Names that already contain dots are preserved verbatim.
	require.Equal("alice.foo.bar", AddUserPrefix("alice", "foo.bar"))
	require.Equal("bob.alice.test", AddUserPrefix("bob", "alice.test"))

	// User names with dots are also preserved.
	require.Equal("al.ice.test", AddUserPrefix("al.ice", "test"))
}

func TestStripUserPrefixMore(t *testing.T) {
	require := require.New(t)

	// Empty user returns name unchanged.
	require.Equal("test", StripUserPrefix("", "test"))
	require.Equal("", StripUserPrefix("", ""))
	require.Equal("alice.test", StripUserPrefix("", "alice.test"))

	// Exact "{user}." prefix is stripped once.
	require.Equal("test", StripUserPrefix("alice", "alice.test"))
	require.Equal("", StripUserPrefix("alice", "alice."))

	// Only one prefix is stripped; nested prefixes remain.
	require.Equal("alice.test", StripUserPrefix("alice", "alice.alice.test"))

	// When the prefix does not match, the name is returned unchanged.
	require.Equal("bob.test", StripUserPrefix("alice", "bob.test"))
	require.Equal("test", StripUserPrefix("alice", "test"))
	require.Equal("alicex.test", StripUserPrefix("alice", "alicex.test"))

	// User with dots is matched literally.
	require.Equal("test", StripUserPrefix("al.ice", "al.ice.test"))
	require.Equal("al.ice.test", StripUserPrefix("al.ice", "al.ice.al.ice.test"))
}

func TestBuildTargetServerProxyNameMore(t *testing.T) {
	require := require.New(t)

	// Empty local user and empty server user: name returned unchanged.
	require.Equal("test", BuildTargetServerProxyName("", "", "test"))
	require.Equal("", BuildTargetServerProxyName("", "", ""))

	// serverUser takes precedence over localUser.
	require.Equal("bob.test", BuildTargetServerProxyName("alice", "bob", "test"))
	require.Equal("bob.", BuildTargetServerProxyName("alice", "bob", ""))

	// localUser is used when serverUser is empty.
	require.Equal("alice.test", BuildTargetServerProxyName("alice", "", "test"))

	// Names containing dots are handled correctly.
	require.Equal("bob.foo.bar", BuildTargetServerProxyName("alice", "bob", "foo.bar"))
}

func TestRoundTripUserPrefix(t *testing.T) {
	require := require.New(t)

	users := []string{"", "alice", "bob", "al.ice"}
	names := []string{"test", "foo.bar", "", "alice.test"}
	for _, user := range users {
		for _, name := range names {
			wireName := AddUserPrefix(user, name)
			// Stripping with the same user must recover the original name.
			require.Equal(name, StripUserPrefix(user, wireName),
				"user=%q name=%q", user, name)
		}
	}
}
