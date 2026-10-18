package util

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRandIDWithLen(t *testing.T) {
	require := require.New(t)

	id, err := RandIDWithLen(0)
	require.NoError(err)
	require.Equal("", id)

	id, err = RandIDWithLen(-1)
	require.NoError(err)
	require.Equal("", id)

	id, err = RandIDWithLen(1)
	require.NoError(err)
	require.Equal(1, len(id))

	id, err = RandIDWithLen(16)
	require.NoError(err)
	require.Equal(16, len(id))

	id, err = RandIDWithLen(32)
	require.NoError(err)
	require.Equal(32, len(id))

	id2, err := RandIDWithLen(16)
	require.NoError(err)
	require.NotEqual(id, id2)

	for _, c := range id {
		require.Contains("0123456789abcdef", string(c))
	}
}

func TestCanonicalAddr(t *testing.T) {
	require := require.New(t)

	require.Equal("example.com", CanonicalAddr("example.com", 80))
	require.Equal("example.com", CanonicalAddr("example.com", 443))
	require.Equal("example.com:8080", CanonicalAddr("example.com", 8080))
	require.Equal("example.com:22", CanonicalAddr("example.com", 22))
	require.Equal("192.168.1.1:8080", CanonicalAddr("192.168.1.1", 8080))
	require.Equal("example.com:0", CanonicalAddr("example.com", 0))
	require.Equal("localhost:8080", CanonicalAddr("localhost", 8080))
}

func TestGenerateResponseErrorString(t *testing.T) {
	require := require.New(t)

	err := errors.New("something went wrong")

	require.Equal("summary", GenerateResponseErrorString("summary", err, false))
	require.Equal("summary", GenerateResponseErrorString("summary", nil, false))
	require.Equal("", GenerateResponseErrorString("", err, false))
	require.Equal("something went wrong", GenerateResponseErrorString("summary", err, true))
	require.Equal("something went wrong", GenerateResponseErrorString("summary", errors.New("something went wrong"), true))
}

func TestRandomSleep(t *testing.T) {
	require := require.New(t)

	d := RandomSleep(time.Second, 0.5, 0.5)
	require.Equal(time.Duration(500)*time.Millisecond, d)

	d = RandomSleep(time.Second, 0.0, 0.0)
	require.Equal(time.Duration(0), d)

	d = RandomSleep(time.Second, 1.0, 1.0)
	require.Equal(time.Second, d)

	d = RandomSleep(time.Nanosecond, 0.0, 1.0)
	require.LessOrEqual(d, time.Nanosecond)
	require.GreaterOrEqual(d, time.Duration(0))

	// d = RandomSleep(time.Hour, 2.0, 1.0)//
	// require.Equal(time.Duration(2)*time.Hour, d)
}

func TestConstantTimeEqString(t *testing.T) {
	require := require.New(t)

	require.True(ConstantTimeEqString("hello", "hello"))
	require.True(ConstantTimeEqString("", ""))
	require.True(ConstantTimeEqString("a", "a"))

	require.False(ConstantTimeEqString("hello", "world"))
	require.False(ConstantTimeEqString("a", "b"))
	require.False(ConstantTimeEqString("hello", "helloo"))
	require.False(ConstantTimeEqString("", "a"))
	require.False(ConstantTimeEqString("abc", "abd"))
}

func TestEmptyOr(t *testing.T) {
	require := require.New(t)

	require.Equal("fallback", EmptyOr("", "fallback"))
	require.Equal("hello", EmptyOr("hello", "fallback"))
	require.Equal("", EmptyOr("", ""))
	require.Equal("x", EmptyOr("x", "y"))

	require.Equal(42, EmptyOr(0, 42))
	require.Equal(7, EmptyOr(7, 42))
	require.Equal(0, EmptyOr(0, 0))

	require.Equal(true, EmptyOr(false, true))
	require.Equal(false, EmptyOr(false, false))
}
