#!/usr/bin/env bash
set -euo pipefail

CADDY_BIN=${CADDY_BIN:-caddy}
BENCH_ROOT=${BENCH_ROOT:-/tmp/varc-real-bench}
FILE_SIZE=${FILE_SIZE:-1G}
TRIALS=${TRIALS:-1}
PRELOADS=${PRELOADS:--1 1 2}
CONCURRENCIES=${CONCURRENCIES:-1 2 4}
ORIGIN_ADDR=${ORIGIN_ADDR:-127.0.0.1:19080}
EDGE_ADDR=${EDGE_ADDR:-127.0.0.1:18080}

origin_dir="$BENCH_ROOT/origin"
results_dir="$BENCH_ROOT/results"
runtime_dir="$BENCH_ROOT/runtime"
mkdir -p "$origin_dir" "$results_dir" "$runtime_dir"

for i in 1 2 3 4; do
	file="$origin_dir/file-$i.bin"
	if [[ ! -f "$file" ]]; then
		fallocate -l "$FILE_SIZE" "$file"
	fi
done

requests_csv="$results_dir/requests.csv"
batches_csv="$results_dir/batches.csv"
printf 'mode,preload,trial,concurrency,file,http_code,ttfb_seconds,total_seconds,speed_bytes_second\n' >"$requests_csv"
printf 'mode,preload,trial,concurrency,makespan_seconds\n' >"$batches_csv"

caddy_pid=''
stop_caddy() {
	if [[ -n "$caddy_pid" ]] && kill -0 "$caddy_pid" 2>/dev/null; then
		kill "$caddy_pid"
		wait "$caddy_pid" 2>/dev/null || true
	fi
	caddy_pid=''
}
trap stop_caddy EXIT

start_caddy() {
	local preload=$1
	local cache_dir=$2
	local config="$runtime_dir/Caddyfile"
	cat >"$config" <<EOF
{
	admin off
	auto_https off
}

http://$ORIGIN_ADDR {
	root * $origin_dir
	file_server
}

http://$EDGE_ADDR {
	route {
		varc http://$ORIGIN_ADDR {
			cache_dir $cache_dir
			append_uri on
			chunk_size 32MiB
			chunk_size_limit 128MiB
			preload_chunks $preload
			poll_interval -1s
			timeout 60s
			probe_timeout 15s
			max_idle_conns 256
			admin_path /_varc
		}
	}
}
EOF
	stop_caddy
	"$CADDY_BIN" run --config "$config" --adapter caddyfile >"$runtime_dir/caddy-$preload.log" 2>&1 &
	caddy_pid=$!
	for _ in $(seq 1 100); do
		if curl -fsSI "http://$ORIGIN_ADDR/file-1.bin" >/dev/null 2>&1; then
			return
		fi
		sleep 0.05
	done
	printf 'Caddy failed to become ready; see %s\n' "$runtime_dir/caddy-$preload.log" >&2
	return 1
}

run_batch() {
	local mode=$1
	local preload=$2
	local trial=$3
	local concurrency=$4
	local base_url=$5
	local started elapsed
	local -a pids=()

	started=$(date +%s%N)
	for i in $(seq 1 "$concurrency"); do
		(
			metrics=$(curl --http1.1 --silent --show-error --output /dev/null \
				--write-out '%{http_code},%{time_starttransfer},%{time_total},%{speed_download}' \
				"$base_url/file-$i.bin")
			printf '%s,%s,%s,%s,%s,%s\n' "$mode" "$preload" "$trial" "$concurrency" "$i" "$metrics" \
				>>"$requests_csv"
		) &
		pids+=("$!")
	done
	for pid in "${pids[@]}"; do
		wait "$pid"
	done
	elapsed=$(( $(date +%s%N) - started ))
	printf '%s,%s,%s,%s,%d.%09d\n' "$mode" "$preload" "$trial" "$concurrency" \
		$((elapsed / 1000000000)) $((elapsed % 1000000000)) >>"$batches_csv"
	printf '%-6s preload=%2s concurrency=%s completed in %d.%03ds\n' \
		"$mode" "$preload" "$concurrency" $((elapsed / 1000000000)) $(((elapsed % 1000000000) / 1000000))
}

baseline_cache="$runtime_dir/cache-baseline"
rm -rf "$baseline_cache"
start_caddy -1 "$baseline_cache"
for concurrency in $CONCURRENCIES; do
	run_batch direct -1 1 "$concurrency" "http://$ORIGIN_ADDR"
done

for preload in $PRELOADS; do
	for trial in $(seq 1 "$TRIALS"); do
		for concurrency in $CONCURRENCIES; do
			cache_dir="$runtime_dir/cache-p${preload}-t${trial}-c${concurrency}"
			rm -rf "$cache_dir"
			start_caddy "$preload" "$cache_dir"
			run_batch cold "$preload" "$trial" "$concurrency" "http://$EDGE_ADDR"
			run_batch warm "$preload" "$trial" "$concurrency" "http://$EDGE_ADDR"
			curl -fsS "http://$EDGE_ADDR/_varc/metrics" >"$results_dir/metrics-p${preload}-t${trial}-c${concurrency}.txt"
			stop_caddy
			rm -rf "$cache_dir"
		done
	done
done

stop_caddy
printf 'Request results: %s\nBatch results: %s\n' "$requests_csv" "$batches_csv"
