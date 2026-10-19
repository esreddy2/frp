//go:build !legacy && !v1

package e2e

import (
	"testing"

	_ "github.com/onsi/ginkgo/v2"

	// test source
	_ "github.com/fatedier/frp/test/e2e/legacy/basic"
	_ "github.com/fatedier/frp/test/e2e/legacy/features"
	_ "github.com/fatedier/frp/test/e2e/legacy/plugin"
	_ "github.com/fatedier/frp/test/e2e/v1/basic"
	_ "github.com/fatedier/frp/test/e2e/v1/features"
	_ "github.com/fatedier/frp/test/e2e/v1/plugin"
)

func TestMain(m *testing.M) {
	RunE2ETestMain(m)
}

func TestE2E(t *testing.T) {
	RunE2ETests(t)
}
