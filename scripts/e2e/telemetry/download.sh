# Sourced by fetch.sh and fetch-interop.sh. Every pinned URL is immutable, so any failed attempt (a curl error such as
# HTTP 404, 429, 5xx or a dropped connection, or bytes that do not match the pinned SHA-256) is retried. All downloads
# of one script share a 150 s deadline, which keeps the worst case inside the callers' 180 s timeout.
FETCH_DEADLINE=$(($(date +%s) + 150))
FETCH_PART=
trap 'rm -f "$FETCH_PART" "$FETCH_PART.zip"' EXIT

# fetch_sha256 <file>: prints the SHA-256 of <file>.
fetch_sha256() {
  local sum
  sum=$(sha256sum "$1")
  echo "${sum%% *}"
}

# fetch_from <source> <want>: makes up to four attempts to place bytes with SHA-256 <want> in $FETCH_PART, with a 2,
# 4, 6 s backoff inside the shared deadline. <source> is a URL, or <zip-url>#<member> for one member of a zip archive.
# On failure sets FETCH_REASON and returns 1.
fetch_from() {
  local url=${1%%#*} member= target=$FETCH_PART attempt left
  if [ "$url" != "$1" ]; then
    member=${1#*#}
    target=$FETCH_PART.zip
  fi
  FETCH_REASON='deadline spent'
  for attempt in 1 2 3 4; do
    left=$((FETCH_DEADLINE - $(date +%s)))
    [ "$left" -gt 0 ] || break
    if curl -fsSL --connect-timeout 10 --max-time $((left < 60 ? left : 60)) -o "$target" "$url"; then
      if [ -z "$member" ] || unzip -p "$target" "$member" >"$FETCH_PART"; then
        if [ "$(fetch_sha256 "$FETCH_PART")" = "$2" ]; then
          rm -f "$FETCH_PART.zip"
          return 0
        fi
        FETCH_REASON='digest mismatch'
      else
        FETCH_REASON="unzip exit $?"
      fi
    else
      FETCH_REASON="curl exit $?"
    fi
    left=$((FETCH_DEADLINE - $(date +%s)))
    [ "$attempt" = 4 ] || [ "$left" -le 0 ] || sleep $((2 * attempt < left ? 2 * attempt : left))
  done
  rm -f "$FETCH_PART.zip"
  return 1
}

# fetch <sums> <dir> <name> <source>...: publishes <dir>/<name> only with the SHA-256 that the sha256sum list <sums>
# pins for <name>. A present file with that digest is kept without network access; any other present file is fetched
# again. Tries each source in order through fetch_from, all into a temporary name beside the file that is renamed only
# after its digest matches, so neither an interrupted download nor wrong bytes reach the cache. On final failure
# prints the file and every source URL with its reason to stderr and returns 1.
fetch() {
  local file=$2/$3 want source failures=
  want=$(awk -v name="$3" '$2 == name { print $1 }' "$1")
  if [ -z "$want" ]; then
    echo "No pinned digest for $3 in $1" >&2
    return 1
  fi
  [ -f "$file" ] && [ "$(fetch_sha256 "$file")" = "$want" ] && return 0
  mkdir -p "$(dirname "$file")"
  FETCH_PART=$(mktemp "$file.part.XXXXXX")
  for source in "${@:4}"; do
    if fetch_from "$source" "$want"; then
      mv "$FETCH_PART" "$file"
      FETCH_PART=
      return 0
    fi
    failures="$failures; from ${source%%#*}: $FETCH_REASON"
  done
  rm -f "$FETCH_PART"
  FETCH_PART=
  echo "Download failed: $file ${failures#; }" >&2
  return 1
}
