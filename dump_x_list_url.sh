#!/usr/bin/env bash
set -euo pipefail

# dump_x_list_url.sh
# Usage:
#   export X_BEARER="YOUR_BEARER_TOKEN"
#   ./dump_x_list_url.sh "https://x.com/i/lists/<LIST_ID>"   # or pass the numeric LIST_ID directly
#
# What this does:
#   - Resolves list id and (best-effort) list name
#   - Streams output as:
#       <basename>_members.jsonl  (one user JSON per line)
#       <basename>_members.csv    (header once, rows appended per page)
#       <basename>.state.json     (checkpoint: next_token, counts, last wait)
#   - Rate-limit aware: reads x-rate-limit-reset and waits until that time
#   - Resumable: if .state.json exists, resumes from saved next_token and appends
#
# Requirements: curl, jq

# ---------- sanity checks ----------
URL="${1:-}"
if [[ -z "$URL" ]]; then
  echo "[error] no URL/ID provided. expected: https://x.com/i/lists/<ID> or <ID>" >&2
  exit 1
fi
if [[ -z "${X_BEARER:-}" ]]; then
  echo "[error] X_BEARER is not set" >&2
  exit 1
fi

need(){ command -v "$1" >/dev/null 2>&1 || { echo "[error] required tool not found: $1" >&2; exit 1; }; }
need curl; need jq; need date

# ---------- resolve LIST_ID ----------
if [[ "$URL" =~ ^https?://[^/]+/i/lists/([0-9]+)$ ]]; then
  LIST_ID="${BASH_REMATCH[1]}"
elif [[ "$URL" =~ ^[0-9]+$ ]]; then
  LIST_ID="$URL"
else
  echo "[error] could not extract numeric list id from input" >&2
  exit 1
fi
echo "[info] list id: ${LIST_ID}"

HOST="https://api.x.com"
UA="dump-x-list/2.0"
HDRS=(-H "Authorization: Bearer ${X_BEARER}" -H "Accept: application/json" -H "User-Agent: ${UA}")
sanitize(){ sed 's#[^A-Za-z0-9._-]#_#g' <<<"$1"; }

# ---------- HTTP helper: capture headers separately, return code ----------
http_get() {
  local pathq="$1" body="$2" head="$3"
  curl -sS -D "$head" "${HDRS[@]}" "${HOST}${pathq}" -o "$body" || true
  awk 'NR==1 {print $2}' "$head"
}

# ---------- Rate-limit wait loop (uses server reset header) ----------
wait_while_429() {
  local code="$1" headf="$2" ctx="$3"
  while [[ "$code" == "429" ]]; do
    local reset now wait_s human
    reset="$(grep -i '^x-rate-limit-reset:' "$headf" | awk '{print $2}' | tr -d '\r' | tail -1)"
    now="$(date +%s)"
    if [[ "$reset" =~ ^[0-9]+$ ]]; then
      wait_s=$(( reset - now + 2 )); (( wait_s < 5 )) && wait_s=5
      human="$(date -u -d "@$reset" +%H:%M:%S 2>/dev/null || date -r "$reset" 2>/dev/null || echo "unknown")"
      echo "[info] ${ctx}: rate-limited (429). waiting ${wait_s}s (until ${human} UTC)…"
    else
      wait_s=60
      echo "[info] ${ctx}: rate-limited (429) without reset header. waiting ${wait_s}s…"
    fi
    # state file update happens in the main loop after we know BASENAME; here we just wait
    sleep "$wait_s"
    return 0   # caller should re-issue request
  done
  return 1     # not 429
}

# ---------- best-effort list name (rate-limit aware) ----------
sleep 2  # small pause to avoid bursting
meta_body="$(mktemp)"; meta_head="$(mktemp)"
pathq="/2/lists/${LIST_ID}"
code="$(http_get "$pathq" "$meta_body" "$meta_head")"
while wait_while_429 "$code" "$meta_head" "list metadata"; do
  : >"$meta_body"; : >"$meta_head"
  code="$(http_get "$pathq" "$meta_body" "$meta_head")"
done

NAME=""
if [[ "$code" == "200" ]] && jq -e . >/dev/null 2>&1 <"$meta_body" && [[ "$(jq -r '.errors? // empty' "$meta_body")" == "" ]]; then
  NAME="$(jq -r '.data.name // empty' "$meta_body")"
fi
if [[ -n "$NAME" ]]; then
  BASENAME="$(sanitize "$NAME")"; echo "[info] list name: ${NAME}"
else
  BASENAME="list_${LIST_ID}"; echo "[info] list name not available (http ${code}); using ${BASENAME}"
fi
rm -f "$meta_body" "$meta_head"

# ---------- paths ----------
OUT_JSONL="${BASENAME}_members.jsonl"
OUT_CSV="${BASENAME}_members.csv"
STATE="${BASENAME}.state.json"

# ---------- state helpers ----------
write_state() {
  local page="$1" total="$2" next_token="$3" last_reset="$4" last_wait="$5" last_event="$6"
  local tmp="${STATE}.tmp"
  jq -n --arg list_id "$LIST_ID" \
        --arg basename "$BASENAME" \
        --arg next_token "$next_token" \
        --arg last_event "$last_event" \
        --argjson page "$page" \
        --argjson total_written "$total" \
        --argjson last_reset_epoch "${last_reset:-0}" \
        --argjson last_wait_seconds "${last_wait:-0}" \
        '{
           list_id: $list_id,
           basename: $basename,
           page: $page,
           total_written: $total_written,
           next_token: ($next_token | (if .=="" then null else . end)),
           last_reset_epoch: $last_reset_epoch,
           last_wait_seconds: $last_wait_seconds,
           last_event: $last_event,
           updated_at: (now | todate)
         }' > "$tmp"
  mv "$tmp" "$STATE"
}

read_state() {
  jq -r '.list_id, .basename, (.page|tostring), (.total_written|tostring), (.next_token // ""), (.last_reset_epoch|tostring), (.last_wait_seconds|tostring)' "$STATE"
}

# ---------- resume if state exists ----------
PAGE=1
TOTAL_WRITTEN=0
NEXT_TOKEN=""

if [[ -f "$STATE" ]]; then
  mapfile -t S < <(read_state)
  st_list="${S[0]}"; st_base="${S[1]}"; st_page="${S[2]}"; st_total="${S[3]}"; st_next="${S[4]}"
  if [[ "$st_list" == "$LIST_ID" ]]; then
    PAGE=$(( st_page )); TOTAL_WRITTEN=$(( st_total )); NEXT_TOKEN="$st_next"
    # sanity: if basename changed (rare), prefer current BASENAME for outputs but keep state continuity
    echo "[info] resuming: page ${PAGE}, total_written ${TOTAL_WRITTEN}, next_token=${NEXT_TOKEN:-<none>}"
  else
    echo "[warn] existing state belongs to list_id=${st_list}; ignoring it and starting fresh for ${LIST_ID}"
  fi
fi

# ensure CSV header exists exactly once
if [[ ! -s "$OUT_CSV" ]]; then
  echo "username,name,id,verified,followers,following" > "$OUT_CSV"
fi
# ensure JSONL file exists
: > /dev/null >> "$OUT_JSONL"

# ---------- paginate members (rate-limit aware, incremental writes, atomic state) ----------
echo "[info] starting member download…"
while :; do
  q="max_results=100&user.fields=id,username,name,verified,public_metrics"
  [[ -n "$NEXT_TOKEN" ]] && q="${q}&pagination_token=${NEXT_TOKEN}"

  body="$(mktemp)"; headf="$(mktemp)"
  pathq="/2/lists/${LIST_ID}/members?${q}"
  code="$(http_get "$pathq" "$body" "$headf")"

  # if rate-limited, record wait info into state and loop until cleared
  while [[ "$code" == "429" ]]; do
    reset="$(grep -i '^x-rate-limit-reset:' "$headf" | awk '{print $2}' | tr -d '\r' | tail -1)"
    now="$(date +%s)"
    if [[ "$reset" =~ ^[0-9]+$ ]]; then
      wait_s=$(( reset - now + 2 )); (( wait_s < 5 )) && wait_s=5
      human="$(date -u -d "@$reset" +%H:%M:%S 2>/dev/null || date -r "$reset" 2>/dev/null || echo "unknown")"
      echo "[info] page ${PAGE}: rate-limited (429). waiting ${wait_s}s (until ${human} UTC)…"
      write_state "$PAGE" "$TOTAL_WRITTEN" "${NEXT_TOKEN}" "$reset" "$wait_s" "rate_limited"
    else
      wait_s=60
      echo "[info] page ${PAGE}: rate-limited (429) without reset header. waiting ${wait_s}s…"
      write_state "$PAGE" "$TOTAL_WRITTEN" "${NEXT_TOKEN}" 0 "$wait_s" "rate_limited_no_header"
    fi
    rm -f "$body" "$headf"
    sleep "$wait_s"
    body="$(mktemp)"; headf="$(mktemp)"
    code="$(http_get "$pathq" "$body" "$headf")"
  done

  # non-200 → explain and stop (state already reflects last successful page)
  if [[ "$code" != "200" ]]; then
    echo "[error] page ${PAGE}: http ${code}. stopping."
    if jq -e . >/dev/null 2>&1 <"$body"; then
      title="$(jq -r '.title? // empty' "$body")"
      detail="$(jq -r '.detail? // empty' "$body")"
      echo "[reason] ${title}${detail:+: ${detail}}"
      jq . "$body" | sed 's/^/[api]/'
    else
      echo "[reason] response was not JSON:"
      head -c 300 "$body" | sed 's/^/[raw]/'
    fi
    rm -f "$body" "$headf"
    exit 1
  fi

  # parse JSON; extract this page's array once
  if ! jq -e . >/dev/null 2>&1 <"$body"; then
    echo "[error] page ${PAGE}: response body not JSON. stopping."
    head -c 300 "$body" | sed 's/^/[raw]/'
    rm -f "$body" "$headf"
    exit 1
  fi

  # ---- incremental writes (JSONL + CSV) ----
  # JSONL: one user per line
  jq -c '.data[]? ' "$body" >> "$OUT_JSONL"

  # CSV: header already ensured; append rows for this page
  jq -r '.data[]? | [
      .username,
      (.name // ""),
      .id,
      (.verified // false),
      (.public_metrics.followers_count // 0),
      (.public_metrics.following_count // 0)
    ] | @csv' "$body" >> "$OUT_CSV"

  # progress
  added="$(jq '(.data // []) | length' "$body")"
  TOTAL_WRITTEN=$(( TOTAL_WRITTEN + added ))
  echo "[info] page ${PAGE}: added ${added}; total ${TOTAL_WRITTEN}"

  # update next token + state (atomically)
  NEXT_TOKEN="$(jq -r '.meta.next_token // ""' "$body")"
  write_state $((PAGE+1)) "$TOTAL_WRITTEN" "$NEXT_TOKEN" 0 0 "page_written"

  rm -f "$body" "$headf"

  # stop if done
  [[ -z "$NEXT_TOKEN" ]] && break

  PAGE=$((PAGE+1))
  sleep 5  # polite spacing between pages
done

echo "[done] wrote ${TOTAL_WRITTEN} members"
echo "[done] jsonl -> ${OUT_JSONL}"
echo "[done] csv   -> ${OUT_CSV}"
echo "[done] state -> ${STATE}"

