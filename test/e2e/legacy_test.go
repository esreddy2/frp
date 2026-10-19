//go:build legacy

package e2e

import (
	"testing"

	_ "github.com/onsi/ginkgo/v2"

	// test source
	_ "github.com/fatedier/frp/test/e2e/legacy/basic"
	_ "github.com/fatedier/frp/test/e2e/legacy/features"
	_ "github.com/fatedier/frp/test/e2e/legacy/plugin"
)

func TestMain(m *testing.M) {
	RunE2ETestMain(m)
}

func TestE2ELegacy(t *testing.T) {
	RunE2ETests(t)
}
