#!/usr/bin/env bash
set -euo pipefail

SRC_DIR="/opt/kite_services/data/strategy_transition_reports"
DEST_DIR="/home/sivapanduri/instance/reports/strategy-transition"

mkdir -p "$DEST_DIR"

LATEST_SRC="$SRC_DIR/strategy_transition_report_latest.html"

if [ ! -f "$LATEST_SRC" ]; then
  echo "Missing latest report: $LATEST_SRC"
  exit 1
fi

RUN_DATE=$(grep -oE 'Run date: [0-9]{4}-[0-9]{2}-[0-9]{2}' "$LATEST_SRC" | head -1 | awk '{print $3}')

if [ -z "$RUN_DATE" ]; then
  echo "Could not detect run date"
  exit 1
fi

cp "$LATEST_SRC" "$DEST_DIR/latest.html"
cp "$LATEST_SRC" "$DEST_DIR/${RUN_DATE}.html"

{
cat <<HTML
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Strategy Transition Report Archive</title>
</head>
<body>
<h1>Strategy Transition Report Archive</h1>
<p>Latest run date: ${RUN_DATE}</p>
<ul>
<li><a href="latest.html">Latest Report</a></li>
HTML

for f in $(ls -1 "$DEST_DIR"/*.html 2>/dev/null | grep -E '/[0-9]{4}-[0-9]{2}-[0-9]{2}\.html$' | sort -r); do
  name=$(basename "$f")
  date="${name%.html}"
  echo "<li><a href=\"$name\">$date</a></li>"
done

cat <<HTML
</ul>
</body>
</html>
HTML
} > "$DEST_DIR/index.html"

echo "Published archive for $RUN_DATE"
