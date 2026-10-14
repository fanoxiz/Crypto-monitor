set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"

go version >/dev/null
docker compose version >/dev/null

project="cryptomonitor-test-$(date +%s)-$$"
compose=(docker compose -f "$root/tests/compose.yaml" -p "$project")

cleanup() {
    status=$?
    trap - EXIT

    if (( status != 0 )); then
        "${compose[@]}" logs --no-color || true
    fi

    if ! "${compose[@]}" down --volumes --remove-orphans; then
        if (( status == 0 )); then
            status=1
        fi
    fi

    exit "$status"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

"${compose[@]}" up --wait --wait-timeout 120

redis_address=$("${compose[@]}" port redis 6379)
postgres_address=$("${compose[@]}" port postgres 5432)

if [[ -z "$redis_address" || -z "$postgres_address" ]]; then
    printf 'Test dependency ports were not published\n' >&2
    exit 1
fi

export TEST_REDIS_ADDR="$redis_address"
export TEST_POSTGRES_DSN="postgres://test_user:test_password@$postgres_address/test_db?sslmode=disable"

go test -race -count=1 -timeout="${TEST_TIMEOUT:-120s}" ./...
