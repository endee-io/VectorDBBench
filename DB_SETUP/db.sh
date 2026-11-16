#!/usr/bin/env bash
# One helper for everything day-to-day.
#
#   ./db.sh start   <db|all>   start (Docker image)        e.g. ./db.sh start qdrant
#   ./db.sh stop    <db|all>   stop, data is kept
#   ./db.sh status             what is running
#   ./db.sh test    [db...]    insert 2 vectors + search, prints PASS/FAIL
#   ./db.sh logs    <db>       follow the logs
#   ./db.sh wipe    <db>       stop AND delete that DB's data
#   ./db.sh versions           pinned image vs newest on Docker Hub, and git status of source clones
#   ./db.sh update             git pull all source clones + re-pull the pinned images
#
#   <db> = milvus | qdrant | redis | vespa
#
# Settings (versions, bind IP, Redis password ...) are in db.env, passed to every docker compose call.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
ENV_FILE=${DB_ENV_FILE:-$HERE/db.env}
[[ -f $ENV_FILE ]] || { echo "missing settings file $ENV_FILE" >&2; exit 1; }
ENV_FILE=$(realpath "$ENV_FILE")   # absolute: docker compose runs inside each database's folder
SRC_ROOT=${SRC_ROOT:-$HOME}        # where the source clones for "versions" / "update" live: $SRC_ROOT/<db>
ALL=(milvus qdrant redis vespa)
CMD=${1:-help}; shift || true

dbs() {  # expand "all" / validate names
  [[ $# -eq 0 ]] && { echo "Which DB? (${ALL[*]} or all)" >&2; exit 1; }
  [[ $1 == all ]] && { echo "${ALL[@]}"; return; }
  for d in "$@"; do [[ -d $HERE/$d ]] || { echo "Unknown DB: $d" >&2; exit 1; }; done
  echo "$@"
}
compose() { (cd "$HERE/$1" && shift && docker compose --env-file "$ENV_FILE" "$@"); }
image_of() { local v; v=$(tr '[:lower:]' '[:upper:]' <<<"$1")_IMAGE; grep -m1 "^$v=" "$ENV_FILE" | cut -d= -f2-; }

wait_ready() {
  local url
  case $1 in
    milvus) url=localhost:9091/healthz ;;
    qdrant) url=localhost:6333/readyz ;;
    vespa)  url=localhost:19071/state/v1/health ;;
    redis)  for _ in $(seq 30); do docker exec redis redis-cli ping 2>/dev/null | grep -q PONG && { echo "  redis ready"; return; }; sleep 1; done; echo "  redis NOT ready"; return ;;
  esac
  for _ in $(seq 60); do curl -sf "$url" >/dev/null && { echo "  $1 ready"; return; }; sleep 2; done
  echo "  $1 NOT ready after 2 min - check: ./db.sh logs $1"
}

case $CMD in
  start)
    bip=$(grep -m1 '^BIND_IP=' "$ENV_FILE" | cut -d= -f2 | awk '{print $1}')
    [[ $bip =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "Set BIND_IP in $ENV_FILE to this server's private IP (hostname -I shows it); now: '$bip'" >&2; exit 1; }
    for d in $(dbs "$@"); do
      echo "Starting $d ($(image_of $d))"
      out=$(compose $d up -d --pull missing 2>&1)
      if grep -qiE 'error|denied' <<<"$out"; then   # docker refused (wrong BIND_IP, port in use ...): say so, don't wait
        grep -iE 'error|denied' <<<"$out" | sort -u | sed 's/^/  /'
        echo "  $d did NOT start - see the message above (README: Troubleshooting)"; continue
      fi
      wait_ready $d
    done
    if [[ " $* " == *" vespa "* || $* == all ]]; then echo "  (vespa: './db.sh test vespa' deploys the sample app; deploy your own app for real data)"; fi
    true ;;
  stop)
    for d in $(dbs "$@"); do echo "Stopping $d"; compose $d down 2>&1 | grep -iE 'error'; done; true ;;
  status)
    docker ps --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}' | grep -E 'NAMES|milvus|qdrant|redis|vespa' || echo "nothing running" ;;
  test)
    "$HERE/smoke-test.sh" "$@" ;;
  logs)
    d=$(dbs "$@"); c=$d; [[ $d == milvus ]] && c=milvus-standalone; docker logs -f --tail 100 "$c" ;;
  wipe)
    for d in $(dbs "$@"); do
      echo "Wiping $d (container + data)"
      compose $d down -v 2>&1 | grep -iE 'error'
      [[ -d $HERE/$d/volumes ]] && sudo rm -rf "${HERE:?}/$d/volumes"
    done; true ;;
  versions)
    printf '%-7s %-34s %-12s %s\n' DB PINNED_IMAGE DOCKER_HUB NEWEST_RELEASE
    for d in "${ALL[@]}"; do
      img=$(image_of $d); repo=${img%:*}; tag=${img##*:}; [[ $repo == */* ]] || repo=library/$repo
      api=https://hub.docker.com/v2/repositories/$repo/tags
      latest=$(curl -s "$api/latest" | jq -r .digest)
      pinned=$(curl -s "$api/$tag" | jq -r .digest)
      prefix=$(sed -E 's/^(v?[0-9]+\.).*/\1/' <<<"$tag")     # same major, e.g. "v3." / "8."
      newest=$(curl -s "$api?page_size=100&name=$prefix" | jq -r --arg dg "$latest" \
        '.results[] | select(.digest==$dg) | .name' | grep -E '^v?[0-9]+\.[0-9]+\.[0-9]+$' | sort -V | tail -1)
      if [[ $latest == "$pinned" ]]; then st="up-to-date"; else st="NEWER"; fi
      printf '%-7s %-34s %-12s %s\n' $d "$img" $st "${newest:-check hub.docker.com/r/$repo (new major?)}"
    done
    echo; printf '%-7s %-10s %-12s %s\n' REPO BRANCH HEAD BEHIND_ORIGIN
    for d in "${ALL[@]}"; do
      r=$SRC_ROOT/$d; [[ -d $r/.git ]] || { echo "$d: no clone at $r"; continue; }
      git -C $r fetch -q 2>/dev/null
      printf '%-7s %-10s %-12s %s\n' $d "$(git -C $r branch --show-current)" "$(git -C $r rev-parse --short HEAD)" \
        "$(git -C $r rev-list --count HEAD..@{u} 2>/dev/null) commits"
    done
    echo; echo "To move to a newer image: change <DB>_IMAGE in db.env, then ./db.sh stop <db> && ./db.sh start <db>"
    ;;
  update)
    for d in "${ALL[@]}"; do
      r=$SRC_ROOT/$d
      if [[ ! -d $r/.git ]]; then echo "$d: no source clone at $r (only needed for source builds) - image re-pulled only"
      elif [[ -n $(git -C $r status --porcelain 2>/dev/null) ]]; then echo "$d: local changes, skipping git pull"
      else echo "$d: $(git -C $r pull --ff-only -q 2>&1 | tail -1) now at $(git -C $r log -1 --format='%h %cd' --date=short)"; fi
      compose $d pull -q
    done
    echo "Images re-pulled. To use them on a running DB: ./db.sh stop <db> && ./db.sh start <db>"
    ;;
  *)
    sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//' ;;
esac
