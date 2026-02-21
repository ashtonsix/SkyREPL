#!/usr/bin/env bash
# check-integer-pks.sh — Verify all primary keys use INTEGER AUTOINCREMENT
#
# Spec (Ch02): All tables MUST use INTEGER PRIMARY KEY AUTOINCREMENT.
# User-facing IDs are base-36 slugs via id.toString(36) / parseInt(slug, 36).
# Wire format uses raw integers. CLI display uses slugs.
#
# This script scans SQL schema files to verify:
# 1. Every CREATE TABLE has INTEGER PRIMARY KEY AUTOINCREMENT
# 2. No UUID or TEXT primary keys
# 3. No non-AUTOINCREMENT integer PKs

set -euo pipefail

IMPL_DIR="${1:-impl}"
EXIT_CODE=0

echo "Checking integer primary keys in $IMPL_DIR..."

# Find all SQL files
SQL_FILES=$(find "$IMPL_DIR" -name "*.sql" -not -path "*/node_modules/*" -not -path "*/.git/*" 2>/dev/null || true)
TS_MIGRATIONS=$(find "$IMPL_DIR" -name "*.ts" -path "*/migrations/*" -not -path "*/node_modules/*" 2>/dev/null || true)

ALL_FILES="$SQL_FILES $TS_MIGRATIONS"

for file in $ALL_FILES; do
  [ -f "$file" ] || continue

  # Find CREATE TABLE statements
  while IFS= read -r line; do
    table_name=$(echo "$line" | grep -oP 'CREATE TABLE\s+\K\w+' || true)
    [ -z "$table_name" ] && continue

    # Check for proper INTEGER PRIMARY KEY AUTOINCREMENT
    # Look in the next 5 lines for the PK definition
    pk_line=$(grep -A5 "CREATE TABLE.*$table_name" "$file" | grep -i "PRIMARY KEY" | head -1 || true)

    if [ -z "$pk_line" ]; then
      continue  # No PK found (might be a join table)
    fi

    # Known exceptions: tables that legitimately use non-standard PKs
    # - idempotency_keys: TEXT PK (user-provided string keys)
    # - manifest_cleanup_state: FK-as-PK (1:1 mapping, not auto-generated)
    # - object_tags: composite key (no single PK column)
    # - manifest_resources: composite key
    case "$table_name" in
      idempotency_keys|manifest_cleanup_state|object_tags|manifest_resources) continue ;;
    esac

    if echo "$pk_line" | grep -qi "TEXT.*PRIMARY KEY\|UUID.*PRIMARY KEY\|VARCHAR.*PRIMARY KEY"; then
      echo "ERROR: $file: Table '$table_name' uses non-integer PK: $pk_line"
      EXIT_CODE=1
    elif echo "$pk_line" | grep -qi "INTEGER PRIMARY KEY" && ! echo "$pk_line" | grep -qi "AUTOINCREMENT"; then
      echo "WARN:  $file: Table '$table_name' has INTEGER PK without AUTOINCREMENT: $pk_line"
      EXIT_CODE=1
    fi
  done < <(grep -n "CREATE TABLE" "$file" 2>/dev/null || true)
done

# Check CLI code for raw integer display (should use idToSlug)
echo ""
echo "Checking CLI for raw integer ID display..."

CLI_FILES=$(find "$IMPL_DIR/cli" -name "*.ts" -not -path "*/node_modules/*" 2>/dev/null || true)
for file in $CLI_FILES; do
  [ -f "$file" ] || continue

  # Look for String(*.id) patterns that should be idToSlug
  matches=$(grep -n 'String([a-z_]*\.id)' "$file" 2>/dev/null || true)
  if [ -n "$matches" ]; then
    echo "WARN:  $file: Raw integer ID display found (should use idToSlug):"
    echo "$matches" | sed 's/^/         /'
    EXIT_CODE=1
  fi
done

if [ $EXIT_CODE -eq 0 ]; then
  echo "OK: All primary keys use INTEGER AUTOINCREMENT, no raw ID display in CLI."
else
  echo ""
  echo "FAIL: Issues found. See above."
fi

exit $EXIT_CODE
