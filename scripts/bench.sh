#!/usr/bin/env bash
# Benchmarks the deployed ma-db-api against representative request shapes,
# including the ones that were previously uncovered by any fast path
# (bpm+random, any audio filter+non-random). Run from inside the cluster —
# e.g. from the subwave controller pod, which already talks to ma-db-api
# over its in-cluster Service DNS name:
#
#   kubectl exec -n subwave deploy/controller -- bash /tmp/bench.sh
#
# (copy this file in first: kubectl cp scripts/bench.sh subwave/<pod>:/tmp/bench.sh)
#
# Requires curl. BASE_URL defaults to the in-cluster ma-db-api address
# subwave's own controller already uses (see controller/src/music/ma-db-api.ts).
set -euo pipefail

BASE_URL="${BASE_URL:-http://ma-db-api.music-assistant.svc.cluster.local:8096}/api/v1"
RUNS="${RUNS:-5}"

# No bc/awk dependency assumed — alpine/distroless images often lack both.
# Collects raw per-run times and reduces with the shell's own arithmetic
# (integer milliseconds) instead.
bench() {
    local label="$1"; local path="$2"
    local times=()
    for _ in $(seq 1 "$RUNS"); do
        local t
        t=$(curl -s -o /dev/null -w '%{time_total}' "${BASE_URL}${path}")
        times+=("$t")
    done
    printf "%-55s %s\n" "$label" "${times[*]}"
}

echo "Benchmarking $BASE_URL ($RUNS runs each)"
echo "-------------------------------------------------------------------"

bench "GET /health"                                          "/health"
bench "GET /tracks?limit=20"                                  "/tracks?limit=20"
bench "GET /tracks?order=random&limit=20"                     "/tracks?order=random&limit=20"
bench "GET /tracks?order=random&limit=20&genre=Ambient"       "/tracks?order=random&limit=20&genre=Ambient"
bench "GET /tracks?order=random&limit=20&energy_min=0.5"      "/tracks?order=random&limit=20&energy_min=0.5"
bench "GET /tracks?order=random&limit=20&bpm_min=120"         "/tracks?order=random&limit=20&bpm_min=120"   # previously uncovered
bench "GET /tracks?limit=20&energy_min=0.5&dir=desc"          "/tracks?limit=20&energy_min=0.5&dir=desc"    # previously uncovered
bench "GET /tracks?order=random&limit=20&bpm_min=100&energy_min=0.3" "/tracks?order=random&limit=20&bpm_min=100&energy_min=0.3"
bench "GET /tracks/observatory (cold or warm)"                "/tracks/observatory"
bench "GET /search?q=love"                                    "/search?q=love&limit=20"
bench "GET /search?q=love (repeat, should hit cache)"         "/search?q=love&limit=20"
bench "GET /search?q=love&types=artists"                      "/search?q=love&limit=20&types=artists"

echo "-------------------------------------------------------------------"
echo "Note: this measures whatever image is currently deployed. To compare"
echo "before/after, run this once before bumping the pinned image digest"
echo "in talos/kubernetes/apps/music-assistant/ma-db-api-deployment.yaml,"
echo "and again after the new image is rolled out."
