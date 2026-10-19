#!/bin/sh
#
# Run the frp e2e suite.
#
# The suites are selected with a build tag, so each one can run on its
# own in CI:
#   (no tag)    — every suite: test/e2e/legacy/... and test/e2e/v1/...
#   legacy      — only the legacy-config suites under test/e2e/legacy/...
#   v1          — only the v1-config suites under test/e2e/v1/...
#
# The Ginkgo CLI supports build tags (-tags), so all three run through
# it and keep the -nodes parallelism. Override with E2E_TAG=legacy or
# E2E_TAG=v1.

SCRIPT=$(readlink -f "$0")
ROOT=$(unset CDPATH && cd "$(dirname "$SCRIPT")/.." && pwd)

# The Ginkgo CLI version must match the ginkgo library version pinned in
# go.mod, otherwise Ginkgo prints a version-mismatch warning and may
# misparse flags. Update this alongside any ginkgo dependency bump.
ginkgoVersion="v2.23.4"

# Ensure the Ginkgo CLI is available; install it if it is not.
if ! command -v ginkgo >/dev/null 2>&1; then
    echo "ginkgo not found, trying to install..."
    go install github.com/onsi/ginkgo/v2/ginkgo@${ginkgoVersion}
fi

# Build tag selecting which e2e suites to run. Empty means all suites.
e2eTag=${E2E_TAG:-}

debug=false
if [ "x${DEBUG}" = "xtrue" ]; then
    debug=true
fi
logLevel=debug
if [ "${LOG_LEVEL}" ]; then
    logLevel="${LOG_LEVEL}"
fi

frpcPath=${ROOT}/bin/frpc
if [ "${FRPC_PATH}" ]; then
    frpcPath="${FRPC_PATH}"
fi
frpsPath=${ROOT}/bin/frps
if [ "${FRPS_PATH}" ]; then
    frpsPath="${FRPS_PATH}"
fi
concurrency="16"
if [ "${CONCURRENCY}" ]; then
    concurrency="${CONCURRENCY}"
fi

commonArgs="-frpc-path=${frpcPath} -frps-path=${frpsPath} -log-level=${logLevel} -debug=${debug}"

if [ -n "${e2eTag}" ]; then
    # Tagged subset: the Ginkgo CLI applies the build tag itself, so the
    # suite keeps the -nodes parallelism.
    ginkgo -tags="${e2eTag}" -nodes=${concurrency} --poll-progress-after=60s \
        ${ROOT}/test/e2e -- ${commonArgs}
else
    ginkgo -nodes=${concurrency} --poll-progress-after=60s \
        ${ROOT}/test/e2e -- ${commonArgs}
fi
