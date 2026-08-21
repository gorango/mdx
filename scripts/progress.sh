#!/usr/bin/env bash
# progress.sh — minimal parallel job pool with a live progress bar.
# Shared by scripts/hydrate_price and scripts/hydrate_orderbook. Source only.
#
# Caller must define before calling pool_run:
#   run_symbol <symbol>  — hydrates one symbol; exit status marks success/failure
#   POOL_LABEL           — short label drawn left of the bar
#
# TTY output:     single line, redrawn in place every POOL_TICK seconds.
# Non-TTY (logs): one status line per completion, so nohup logs stay readable.

POOL_TICK="${POOL_TICK:-2}"
BAR_WIDTH="${BAR_WIDTH:-32}"

_POOL_WORKER_PIDS=()
_POOL_STOP_FILE=""

# Ctrl+C / kill: bash would otherwise treat a child's SIGINT death as "the
# user interrupted the child, not me", return from wait -n, and the dispatch
# loop would spawn the NEXT wave of workers — the pool becomes unkillable.
# Instead: kill every worker and its children, release the watchdog, exit.
_pool_handle_signal() {
	trap - INT TERM
	local pid
	printf '\n%s interrupted (%s) — stopping %d worker(s)\n' \
		"${POOL_LABEL:-pool}" "$1" "${#_POOL_WORKER_PIDS[@]}" >&2
	for pid in "${_POOL_WORKER_PIDS[@]}"; do
		# Leaf first (the hydrator binary), then the worker subshell itself.
		pkill -TERM -P "$pid" 2>/dev/null
		kill -TERM "$pid" 2>/dev/null
	done
	[[ -n $_POOL_STOP_FILE ]] && touch "$_POOL_STOP_FILE"
	exit 130
}

_pool_status_counts() { # $1=status file -> sets _OK, _FAIL, _DONE
	_OK=$(grep -c '^ok' "$1" 2>/dev/null || true)
	_FAIL=$(grep -c '^fail' "$1" 2>/dev/null || true)
	_DONE=$((_OK + _FAIL))
}

_pool_fmt_clock() { # seconds -> hh:mm:ss
	printf '%02d:%02d:%02d' $(($1 / 3600)) $(($1 % 3600 / 60)) $(($1 % 60))
}

_pool_line() { # done total ok fail start_ts -> one status line, no newline
	local done=$1 total=$2 ok=$3 fail=$4 start_ts=$5
	local pct=0 fill=0 i bar='' elapsed now eta='-'
	now=$(date +%s)
	if ((total > 0)); then
		pct=$((done * 100 / total))
		fill=$((BAR_WIDTH * done / total))
	fi
	for ((i = 0; i < fill; i++)); do bar+='█'; done
	for ((i = fill; i < BAR_WIDTH; i++)); do bar+='░'; done
	elapsed=$((now - start_ts))
	if ((done > 0 && done < total)); then
		eta=$(_pool_fmt_clock $((elapsed * (total - done) / done)))
	fi
	printf '%s [%s] %d/%d %d%% ok=%d fail=%d elapsed %s eta %s' \
		"$POOL_LABEL" "$bar" "$done" "$total" "$pct" "$ok" "$fail" \
		"$(_pool_fmt_clock "$elapsed")" "$eta"
}

_pool_watchdog() { # $1=status file $2=total $3=start_ts $4=stop file
	local last=-1
	while [[ ! -f $4 ]]; do
		_pool_status_counts "$1"
		if [[ -t 1 ]]; then
			printf '\r\033[K'
			_pool_line "$_DONE" "$2" "$_OK" "$_FAIL" "$3"
		elif ((_DONE != last)); then
			_pool_line "$_DONE" "$2" "$_OK" "$_FAIL" "$3"
			printf '\n'
			last=$_DONE
		fi
		sleep "$POOL_TICK"
	done
}

_pool_worker() { # $1=symbol $2=status file $3=log dir
	local sym=$1 logf
	logf="$3/$(printf '%s' "$1" | tr '/:' '__').log"
	if run_symbol "$sym" >"$logf" 2>&1; then
		echo "ok $sym" >>"$2"
	else
		echo "fail $sym" >>"$2"
	fi
}

# pool_run TOTAL WORKERS STATUS_FILE LOG_DIR SYMBOL...
# Runs run_symbol for each symbol, WORKERS at a time. Returns non-zero if any failed.
pool_run() {
	local total=$1 workers=$2 status_file=$3 log_dir=$4
	shift 4
	local start_ts stop_file wdpid running=0 sym

	start_ts=$(date +%s)
	stop_file="$status_file.stop"
	: >"$status_file"

	_POOL_STOP_FILE="$stop_file"
	trap '_pool_handle_signal INT' INT
	trap '_pool_handle_signal TERM' TERM

	_pool_watchdog "$status_file" "$total" "$start_ts" "$stop_file" &
	wdpid=$!

	for sym in "$@"; do
		while ((running >= workers)); do
			wait -n 2>/dev/null || true
			running=$((running - 1))
		done
		_pool_worker "$sym" "$status_file" "$log_dir" &
		_POOL_WORKER_PIDS+=($!)
		running=$((running + 1))
	done
	while ((running > 0)); do
		wait -n 2>/dev/null || true
		running=$((running - 1))
	done

	trap - INT TERM

	touch "$stop_file"
	wait "$wdpid" 2>/dev/null || true

	_pool_status_counts "$status_file"
	if [[ -t 1 ]]; then
		printf '\r\033[K'
	fi
	_pool_line "$_DONE" "$total" "$_OK" "$_FAIL" "$start_ts"
	printf '\n'
	((_FAIL == 0))
}

# pool_failures STATUS_FILE -> failing symbols, one per line
pool_failures() {
	grep '^fail ' "$1" 2>/dev/null | cut -d' ' -f2- || true
}
