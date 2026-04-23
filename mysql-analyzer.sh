#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# mysql-analyzer.sh — MySQL Instance-Wide Performance Analyzer
#
# Usage:
#   ./mysql-analyzer.sh -h <host> -P <port> -u <user> -p <password> [-d <db>] [-o <output>]
#
# Options:
#   -h  Host (required)
#   -P  Port (default: 3306)
#   -u  Username (required)
#   -p  Password (required)
#   -d  Target database (optional, analyzes all if omitted)
#   -o  Output file (default: mysql-analysis-YYYY-MM-DD.md)
#
# Features auto-detected:
#   - performance_schema
#   - sys schema
#   - information_schema (always available)
#   - slow_query_log status
#   - InnoDB status
###############################################################################

# ─── Defaults ────────────────────────────────────────────────────────────────
DB_HOST=""
DB_PORT="3306"
DB_USER=""
DB_PASS=""
TARGET_DB=""
OUTPUT=""
SYSTEM_DBS="'information_schema','performance_schema','mysql','sys','mysql_audit'"

# ─── Feature flags ───────────────────────────────────────────────────────────
HAS_PERF_SCHEMA=0
HAS_SYS=0
HAS_SLOW_LOG=0

# ─── Colors ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

# ─── Parse args ──────────────────────────────────────────────────────────────
while getopts "h:P:u:p:d:o:" opt; do
  case $opt in
    h) DB_HOST="$OPTARG" ;;
    P) DB_PORT="$OPTARG" ;;
    u) DB_USER="$OPTARG" ;;
    p) DB_PASS="$OPTARG" ;;
    d) TARGET_DB="$OPTARG" ;;
    o) OUTPUT="$OPTARG" ;;
    *) echo "Usage: $0 -h <host> -P <port> -u <user> -p <password> [-d <db>] [-o <output>]"; exit 1 ;;
  esac
done

if [[ -z "$DB_HOST" || -z "$DB_USER" || -z "$DB_PASS" ]]; then
  echo -e "${RED}Error: -h, -u, -p are required${NC}"
  echo "Usage: $0 -h <host> -P <port> -u <user> -p <password> [-d <db>] [-o <output>]"
  exit 1
fi

if [[ -z "$OUTPUT" ]]; then
  OUTPUT="mysql-analysis-$(date +%Y-%m-%d).md"
fi

# ─── MySQL wrapper ───────────────────────────────────────────────────────────
run_sql() {
  mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASS" \
    --batch --skip-column-names -e "$1" 2>/dev/null
}

run_sql_with_headers() {
  mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASS" \
    --batch -e "$1" 2>/dev/null
}

# Convert TSV to markdown table
tsv_to_md() {
  local input="$1"
  if [[ -z "$input" ]]; then
    echo "(no data)"
    return
  fi
  local header
  header=$(echo "$input" | head -1)
  local ncols
  ncols=$(echo "$header" | awk -F'\t' '{print NF}')
  # header row
  echo "$header" | awk -F'\t' '{printf "| "; for(i=1;i<=NF;i++) printf "%s | ", $i; print ""}'
  # separator
  printf "|"
  for ((i=0; i<ncols; i++)); do printf " --- |"; done
  echo ""
  # data rows
  echo "$input" | tail -n +2 | awk -F'\t' '{printf "| "; for(i=1;i<=NF;i++) printf "%s | ", $i; print ""}'
}

# ─── Progress helpers ────────────────────────────────────────────────────────
step() {
  echo -e "${CYAN}[$(date +%H:%M:%S)]${NC} $1" >&2
}

ok() {
  echo -e "  ${GREEN}✓${NC} $1" >&2
}

warn() {
  echo -e "  ${YELLOW}⚠${NC} $1" >&2
}

fail() {
  echo -e "  ${RED}✗${NC} $1" >&2
}

###############################################################################
# PHASE 0: Connection Test
###############################################################################
step "Connecting to $DB_HOST:$DB_PORT..."

VERSION=$(run_sql "SELECT VERSION();" 2>/dev/null) || {
  echo -e "${RED}Failed to connect to MySQL${NC}"
  exit 1
}
ok "Connected — MySQL $VERSION"

###############################################################################
# PHASE 1: Feature Detection
###############################################################################
step "Detecting available features..."

# performance_schema
PS_STATUS=$(run_sql "SELECT @@performance_schema;" 2>/dev/null || echo "0")
if [[ "$PS_STATUS" == "1" ]]; then
  HAS_PERF_SCHEMA=1
  ok "performance_schema: ON"
else
  warn "performance_schema: OFF — index/query analysis will be limited"
fi

# sys schema
SYS_CHECK=$(run_sql "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME='sys';" 2>/dev/null || echo "0")
if [[ "$SYS_CHECK" == "1" ]]; then
  HAS_SYS=1
  ok "sys schema: available"
else
  warn "sys schema: not available"
fi

# slow_query_log
SLOW_LOG=$(run_sql "SELECT @@slow_query_log;" 2>/dev/null || echo "0")
if [[ "$SLOW_LOG" == "1" ]]; then
  HAS_SLOW_LOG=1
  ok "slow_query_log: ON"
else
  warn "slow_query_log: OFF"
fi

# InnoDB
INNODB_CHECK=$(run_sql "SELECT COUNT(*) FROM information_schema.ENGINES WHERE ENGINE='InnoDB' AND SUPPORT IN ('YES','DEFAULT');" 2>/dev/null || echo "0")
if [[ "$INNODB_CHECK" -ge 1 ]]; then
  ok "InnoDB: available"
else
  warn "InnoDB: not available"
fi

# Check available performance_schema consumers
if [[ $HAS_PERF_SCHEMA -eq 1 ]]; then
  STMT_DIGEST=$(run_sql "SELECT COUNT(*) FROM performance_schema.events_statements_summary_by_digest LIMIT 1;" 2>/dev/null || echo "0")
  IO_WAITS=$(run_sql "SELECT COUNT(*) FROM performance_schema.table_io_waits_summary_by_index_usage LIMIT 1;" 2>/dev/null || echo "0")
  if [[ "$STMT_DIGEST" != "0" ]]; then ok "Statement digest: has data"; else warn "Statement digest: empty"; fi
  if [[ "$IO_WAITS" != "0" ]]; then ok "Table I/O waits: has data"; else warn "Table I/O waits: empty"; fi
fi

echo ""
step "Starting analysis... (output: $OUTPUT)"
echo ""

###############################################################################
# PHASE 2: Begin Report
###############################################################################

{
echo "# MySQL Instance Analysis Report"
echo ""
echo "> Generated: $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo "> Host: $DB_HOST:$DB_PORT"
echo "> MySQL Version: $VERSION"
echo "> Features: performance_schema=$(if [[ $HAS_PERF_SCHEMA -eq 1 ]]; then echo ON; else echo OFF; fi), sys=$(if [[ $HAS_SYS -eq 1 ]]; then echo ON; else echo OFF; fi), slow_log=$(if [[ $HAS_SLOW_LOG -eq 1 ]]; then echo ON; else echo OFF; fi)"
echo ""
echo "---"
echo ""

###############################################################################
# Section 1: Instance Overview
###############################################################################
step "Analyzing instance overview..."

echo "## 1. Instance Overview"
echo ""

# 1-1. Database sizes
echo "### 1-1. Database Sizes"
echo ""
RESULT=$(run_sql_with_headers "
SELECT
  TABLE_SCHEMA AS \`Database\`,
  COUNT(*) AS \`Tables\`,
  ROUND(SUM(DATA_LENGTH)/1024/1024, 2) AS \`Data_MB\`,
  ROUND(SUM(INDEX_LENGTH)/1024/1024, 2) AS \`Index_MB\`,
  ROUND(SUM(DATA_LENGTH + INDEX_LENGTH)/1024/1024, 2) AS \`Total_MB\`,
  SUM(TABLE_ROWS) AS \`Total_Rows\`
FROM information_schema.TABLES
WHERE TABLE_SCHEMA NOT IN ($SYSTEM_DBS)
GROUP BY TABLE_SCHEMA
ORDER BY SUM(DATA_LENGTH + INDEX_LENGTH) DESC;
")
tsv_to_md "$RESULT"
echo ""

# 1-2. Global status (key metrics)
echo "### 1-2. Key Global Status"
echo ""
RESULT=$(run_sql_with_headers "
SELECT VARIABLE_NAME AS \`Variable\`, VARIABLE_VALUE AS \`Value\`
FROM performance_schema.global_status
WHERE VARIABLE_NAME IN (
  'Innodb_buffer_pool_reads','Innodb_buffer_pool_read_requests',
  'Innodb_buffer_pool_pages_total','Innodb_buffer_pool_pages_free',
  'Innodb_buffer_pool_pages_dirty','Innodb_buffer_pool_wait_free',
  'Innodb_row_lock_waits','Innodb_row_lock_time','Innodb_row_lock_time_avg',
  'Innodb_row_lock_time_max','Innodb_deadlocks',
  'Threads_connected','Threads_running',
  'Slow_queries','Created_tmp_disk_tables','Created_tmp_tables',
  'Select_full_join','Select_scan','Sort_merge_passes',
  'Table_locks_waited','Innodb_log_waits',
  'Connections','Aborted_connects','Aborted_clients'
)
ORDER BY VARIABLE_NAME;
")
tsv_to_md "$RESULT"
echo ""

# 1-3. Buffer pool
echo "### 1-3. Buffer Pool"
echo ""
BP_SIZE=$(run_sql "SELECT ROUND(@@innodb_buffer_pool_size/1024/1024, 0);")
BP_TOTAL=$(run_sql "SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Innodb_buffer_pool_pages_total';")
BP_FREE=$(run_sql "SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Innodb_buffer_pool_pages_free';")
BP_DIRTY=$(run_sql "SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Innodb_buffer_pool_pages_dirty';")
if [[ -n "$BP_TOTAL" && "$BP_TOTAL" != "0" ]]; then
  BP_USED_PCT=$(awk "BEGIN {printf \"%.1f\", (($BP_TOTAL - $BP_FREE) / $BP_TOTAL) * 100}")
  BP_HIT_REQ=$(run_sql "SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Innodb_buffer_pool_read_requests';")
  BP_DISK=$(run_sql "SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Innodb_buffer_pool_reads';")
  if [[ -n "$BP_HIT_REQ" && "$BP_HIT_REQ" != "0" ]]; then
    BP_HIT_RATE=$(awk "BEGIN {printf \"%.4f\", (1 - $BP_DISK / $BP_HIT_REQ) * 100}")
  else
    BP_HIT_RATE="N/A"
  fi
else
  BP_USED_PCT="N/A"
  BP_HIT_RATE="N/A"
fi
echo "- Size: ${BP_SIZE}MB"
echo "- Pages total: $BP_TOTAL / free: $BP_FREE / dirty: $BP_DIRTY"
echo "- Usage: ${BP_USED_PCT}%"
echo "- Hit rate: ${BP_HIT_RATE}%"
echo ""

# 1-4. Server variables (tuning-relevant)
echo "### 1-4. Key Server Variables"
echo ""
RESULT=$(run_sql_with_headers "
SELECT VARIABLE_NAME AS \`Variable\`, VARIABLE_VALUE AS \`Value\`
FROM performance_schema.global_variables
WHERE VARIABLE_NAME IN (
  'innodb_buffer_pool_size','innodb_buffer_pool_instances',
  'innodb_log_file_size','innodb_log_buffer_size',
  'innodb_flush_log_at_trx_commit','innodb_flush_method',
  'innodb_io_capacity','innodb_io_capacity_max',
  'innodb_read_io_threads','innodb_write_io_threads',
  'innodb_thread_concurrency','innodb_adaptive_hash_index',
  'innodb_file_per_table','innodb_lock_wait_timeout',
  'innodb_redo_log_capacity',
  'max_connections','thread_cache_size',
  'table_open_cache','table_definition_cache',
  'sort_buffer_size','join_buffer_size',
  'read_buffer_size','read_rnd_buffer_size',
  'tmp_table_size','max_heap_table_size',
  'long_query_time','slow_query_log',
  'log_queries_not_using_indexes'
)
ORDER BY VARIABLE_NAME;
")
tsv_to_md "$RESULT"
echo ""

ok "Instance overview done"


###############################################################################
# Section 2: Per-Database Query Load (performance_schema)
###############################################################################

if [[ $HAS_PERF_SCHEMA -eq 1 ]]; then
  step "Analyzing per-database query load..."

  echo "## 2. Per-Database Query Load"
  echo ""

  # 2-1. Query stats per DB
  echo "### 2-1. Query Statistics by Database"
  echo ""
  RESULT=$(run_sql_with_headers "
  SELECT
    SCHEMA_NAME AS \`Database\`,
    COUNT(*) AS \`Unique_Queries\`,
    SUM(COUNT_STAR) AS \`Total_Executions\`,
    ROUND(SUM(SUM_TIMER_WAIT)/1000000000000, 2) AS \`Total_Time_sec\`,
    ROUND(SUM(SUM_TIMER_WAIT)/1000000000000 / NULLIF(SUM(COUNT_STAR),0), 4) AS \`Avg_Time_sec\`,
    SUM(SUM_ROWS_EXAMINED) AS \`Rows_Examined\`,
    SUM(SUM_NO_INDEX_USED) AS \`No_Index_Used\`,
    SUM(SUM_CREATED_TMP_DISK_TABLES) AS \`Tmp_Disk_Tables\`,
    SUM(SUM_SORT_MERGE_PASSES) AS \`Sort_Merge_Passes\`
  FROM performance_schema.events_statements_summary_by_digest
  WHERE SCHEMA_NAME IS NOT NULL
    AND SCHEMA_NAME NOT IN ($SYSTEM_DBS)
  GROUP BY SCHEMA_NAME
  ORDER BY SUM(SUM_TIMER_WAIT) DESC;
  ")
  tsv_to_md "$RESULT"
  echo ""

  # 2-2. Table I/O per DB
  echo "### 2-2. Table I/O by Database"
  echo ""
  RESULT=$(run_sql_with_headers "
  SELECT
    OBJECT_SCHEMA AS \`Database\`,
    COUNT(DISTINCT OBJECT_NAME) AS \`Tables\`,
    SUM(COUNT_READ) AS \`Total_Reads\`,
    ROUND(SUM(SUM_TIMER_READ)/1000000000000, 2) AS \`Read_Time_sec\`,
    SUM(COUNT_WRITE) AS \`Total_Writes\`,
    ROUND(SUM(SUM_TIMER_WRITE)/1000000000000, 2) AS \`Write_Time_sec\`
  FROM performance_schema.table_io_waits_summary_by_table
  WHERE OBJECT_SCHEMA NOT IN ($SYSTEM_DBS)
  GROUP BY OBJECT_SCHEMA
  ORDER BY SUM(SUM_TIMER_WAIT) DESC;
  ")
  tsv_to_md "$RESULT"
  echo ""

  # 2-3. Full scan per DB
  echo "### 2-3. Full Table Scans by Database"
  echo ""
  RESULT=$(run_sql_with_headers "
  SELECT
    OBJECT_SCHEMA AS \`Database\`,
    SUM(COUNT_READ) AS \`Full_Scan_Reads\`,
    ROUND(SUM(SUM_TIMER_READ)/1000000000000, 2) AS \`Full_Scan_Time_sec\`
  FROM performance_schema.table_io_waits_summary_by_index_usage
  WHERE INDEX_NAME IS NULL AND COUNT_READ > 0
    AND OBJECT_SCHEMA NOT IN ($SYSTEM_DBS)
  GROUP BY OBJECT_SCHEMA
  ORDER BY SUM(SUM_TIMER_READ) DESC;
  ")
  tsv_to_md "$RESULT"
  echo ""

  ok "Per-database query load done"
fi


###############################################################################
# Section 3: Top Tables (instance-wide)
###############################################################################
step "Analyzing top tables..."

echo "## 3. Top Tables by Size (Instance-Wide)"
echo ""
RESULT=$(run_sql_with_headers "
SELECT
  TABLE_SCHEMA AS \`Database\`,
  TABLE_NAME AS \`Table\`,
  ROUND(DATA_LENGTH/1024/1024, 2) AS \`Data_MB\`,
  ROUND(INDEX_LENGTH/1024/1024, 2) AS \`Index_MB\`,
  ROUND((DATA_LENGTH + INDEX_LENGTH)/1024/1024, 2) AS \`Total_MB\`,
  TABLE_ROWS AS \`Rows\`
FROM information_schema.TABLES
WHERE TABLE_SCHEMA NOT IN ($SYSTEM_DBS)
ORDER BY (DATA_LENGTH + INDEX_LENGTH) DESC
LIMIT 25;
")
tsv_to_md "$RESULT"
echo ""

if [[ $HAS_PERF_SCHEMA -eq 1 ]]; then
  echo "### Top Tables by I/O Wait"
  echo ""
  RESULT=$(run_sql_with_headers "
  SELECT
    OBJECT_SCHEMA AS \`Database\`,
    OBJECT_NAME AS \`Table\`,
    SUM(COUNT_READ) AS \`Reads\`,
    ROUND(SUM(SUM_TIMER_READ)/1000000000000, 2) AS \`Read_Time_sec\`,
    SUM(COUNT_WRITE) AS \`Writes\`,
    ROUND(SUM(SUM_TIMER_WRITE)/1000000000000, 2) AS \`Write_Time_sec\`
  FROM performance_schema.table_io_waits_summary_by_table
  WHERE OBJECT_SCHEMA NOT IN ($SYSTEM_DBS)
  GROUP BY OBJECT_SCHEMA, OBJECT_NAME
  ORDER BY SUM(SUM_TIMER_WAIT) DESC
  LIMIT 20;
  ")
  tsv_to_md "$RESULT"
  echo ""

  echo "### Top Tables by Full Scan"
  echo ""
  RESULT=$(run_sql_with_headers "
  SELECT
    OBJECT_SCHEMA AS \`Database\`,
    OBJECT_NAME AS \`Table\`,
    COUNT_READ AS \`Full_Scan_Reads\`,
    ROUND(SUM_TIMER_READ/1000000000000, 2) AS \`Full_Scan_Time_sec\`
  FROM performance_schema.table_io_waits_summary_by_index_usage
  WHERE INDEX_NAME IS NULL AND COUNT_READ > 0
    AND OBJECT_SCHEMA NOT IN ($SYSTEM_DBS)
  ORDER BY SUM_TIMER_READ DESC
  LIMIT 20;
  ")
  tsv_to_md "$RESULT"
  echo ""
fi

ok "Top tables done"


###############################################################################
# Section 4: Index Analysis (per target DB or all DBs)
###############################################################################

analyze_indexes_for_db() {
  local db="$1"
  local escaped_db
  escaped_db=$(echo "$db" | sed "s/'/\\\\'/g")

  echo "### Indexes: \`$db\`"
  echo ""

  if [[ $HAS_PERF_SCHEMA -eq 1 ]]; then
    # Unused indexes
    local unused_count
    unused_count=$(run_sql "
    SELECT COUNT(*)
    FROM performance_schema.table_io_waits_summary_by_index_usage
    WHERE OBJECT_SCHEMA = '$escaped_db'
      AND INDEX_NAME IS NOT NULL
      AND INDEX_NAME != 'PRIMARY'
      AND COUNT_READ = 0;
    ")

    echo "#### Unused Indexes ($unused_count found)"
    echo ""
    if [[ "$unused_count" -gt 0 ]]; then
      echo "> These indexes have COUNT_READ = 0 since last server restart. Write overhead only."
      if [[ "$unused_count" -gt 50 ]]; then
        echo "> Showing first 50 of $unused_count. Use \`-d $db\` for full listing."
      fi
      echo ""
      local unused
      unused=$(run_sql_with_headers "
      SELECT
        OBJECT_NAME AS \`Table\`,
        INDEX_NAME AS \`Index\`,
        COUNT_WRITE AS \`Write_Count\`
      FROM performance_schema.table_io_waits_summary_by_index_usage
      WHERE OBJECT_SCHEMA = '$escaped_db'
        AND INDEX_NAME IS NOT NULL
        AND INDEX_NAME != 'PRIMARY'
        AND COUNT_READ = 0
      ORDER BY OBJECT_NAME, INDEX_NAME
      LIMIT 50;
      ")
      tsv_to_md "$unused"
    else
      echo "(none)"
    fi
    echo ""

    # Per-table index I/O
    echo "#### Index I/O (Top 20 by read time)"
    echo ""
    RESULT=$(run_sql_with_headers "
    SELECT
      OBJECT_NAME AS \`Table\`,
      INDEX_NAME AS \`Index\`,
      COUNT_READ AS \`Reads\`,
      ROUND(SUM_TIMER_READ/1000000000000, 2) AS \`Read_Time_sec\`,
      COUNT_WRITE AS \`Writes\`,
      ROUND(SUM_TIMER_WRITE/1000000000000, 4) AS \`Write_Time_sec\`
    FROM performance_schema.table_io_waits_summary_by_index_usage
    WHERE OBJECT_SCHEMA = '$escaped_db'
      AND INDEX_NAME IS NOT NULL
      AND (COUNT_READ > 0 OR COUNT_WRITE > 0)
    ORDER BY SUM_TIMER_READ DESC
    LIMIT 20;
    ")
    tsv_to_md "$RESULT"
    echo ""
  fi

  # Duplicate indexes (same first column)
  echo "#### Potential Duplicate Indexes (same leading column)"
  echo ""
  RESULT=$(run_sql_with_headers "
  SELECT
    t1.TABLE_NAME AS \`Table\`,
    t1.INDEX_NAME AS \`Index_1\`,
    t2.INDEX_NAME AS \`Index_2\`,
    t1.COLUMN_NAME AS \`Shared_Leading_Column\`
  FROM information_schema.STATISTICS t1
  JOIN information_schema.STATISTICS t2
    ON t1.TABLE_SCHEMA = t2.TABLE_SCHEMA
    AND t1.TABLE_NAME = t2.TABLE_NAME
    AND t1.SEQ_IN_INDEX = t2.SEQ_IN_INDEX
    AND t1.COLUMN_NAME = t2.COLUMN_NAME
    AND t1.INDEX_NAME != t2.INDEX_NAME
    AND t1.INDEX_NAME < t2.INDEX_NAME
  WHERE t1.TABLE_SCHEMA = '$escaped_db'
    AND t1.SEQ_IN_INDEX = 1
  ORDER BY t1.TABLE_NAME
  LIMIT 30;
  ")
  local dup_count
  dup_count=$(echo "$RESULT" | tail -n +2 | awk 'NF{c++}END{print c+0}')
  if [[ "$dup_count" -gt 0 ]]; then
    tsv_to_md "$RESULT"
  else
    echo "(none)"
  fi
  echo ""

  # All indexes listing (limit to 100 for large DBs)
  local idx_total
  idx_total=$(run_sql "SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = '$escaped_db';")
  echo "#### All Indexes (total: $idx_total)"
  echo ""
  if [[ "$idx_total" -gt 200 ]]; then
    echo "> Showing first 100 of $idx_total index entries. Use \`-d $db\` for full listing."
    echo ""
  fi
  RESULT=$(run_sql_with_headers "
  SELECT
    TABLE_NAME AS \`Table\`,
    INDEX_NAME AS \`Index\`,
    SEQ_IN_INDEX AS \`Seq\`,
    COLUMN_NAME AS \`Column\`,
    CARDINALITY AS \`Cardinality\`,
    NULLABLE AS \`Nullable\`
  FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA = '$escaped_db'
  ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX
  LIMIT 100;
  ")
  tsv_to_md "$RESULT"
  echo ""
}

step "Analyzing indexes..."
echo "## 4. Index Analysis"
echo ""

if [[ -n "$TARGET_DB" ]]; then
  analyze_indexes_for_db "$TARGET_DB"
else
  # Get list of user DBs with tables, sorted by size
  DBS=$(run_sql "
  SELECT TABLE_SCHEMA
  FROM information_schema.TABLES
  WHERE TABLE_SCHEMA NOT IN ($SYSTEM_DBS)
  GROUP BY TABLE_SCHEMA
  ORDER BY SUM(DATA_LENGTH + INDEX_LENGTH) DESC;
  ")
  while IFS= read -r db; do
    [[ -z "$db" ]] && continue
    analyze_indexes_for_db "$db"
  done <<< "$DBS"
fi

# Unused indexes summary per DB
if [[ $HAS_PERF_SCHEMA -eq 1 ]]; then
  echo "### Unused Indexes Summary (All Databases)"
  echo ""
  RESULT=$(run_sql_with_headers "
  SELECT
    OBJECT_SCHEMA AS \`Database\`,
    COUNT(*) AS \`Unused_Index_Count\`
  FROM performance_schema.table_io_waits_summary_by_index_usage
  WHERE INDEX_NAME IS NOT NULL
    AND INDEX_NAME != 'PRIMARY'
    AND COUNT_READ = 0
    AND OBJECT_SCHEMA NOT IN ($SYSTEM_DBS)
  GROUP BY OBJECT_SCHEMA
  ORDER BY COUNT(*) DESC;
  ")
  tsv_to_md "$RESULT"
  echo ""
fi

ok "Index analysis done"


###############################################################################
# Section 5: Query Analysis (performance_schema)
###############################################################################

analyze_queries_for_db() {
  local db="$1"
  local escaped_db
  escaped_db=$(echo "$db" | sed "s/'/\\\\'/g")

  echo "### Queries: \`$db\`"
  echo ""

  # 5-1. Slowest queries by max time
  echo "#### Slowest Queries (by max execution time)"
  echo ""
  RESULT=$(run_sql_with_headers "
  SELECT
    LEFT(DIGEST_TEXT, 200) AS \`Query_Digest\`,
    COUNT_STAR AS \`Exec_Count\`,
    ROUND(SUM_TIMER_WAIT/1000000000000, 2) AS \`Total_Time_sec\`,
    ROUND(AVG_TIMER_WAIT/1000000000000, 4) AS \`Avg_Time_sec\`,
    ROUND(MAX_TIMER_WAIT/1000000000000, 2) AS \`Max_Time_sec\`,
    SUM_ROWS_EXAMINED AS \`Rows_Examined\`,
    SUM_ROWS_SENT AS \`Rows_Sent\`
  FROM performance_schema.events_statements_summary_by_digest
  WHERE SCHEMA_NAME = '$escaped_db'
  ORDER BY MAX_TIMER_WAIT DESC
  LIMIT 15;
  ")
  tsv_to_md "$RESULT"
  echo ""

  # 5-2. Queries with no index used
  echo "#### Queries Without Index (NO_INDEX_USED > 0)"
  echo ""
  RESULT=$(run_sql_with_headers "
  SELECT
    LEFT(DIGEST_TEXT, 200) AS \`Query_Digest\`,
    COUNT_STAR AS \`Exec_Count\`,
    ROUND(SUM_TIMER_WAIT/1000000000000, 2) AS \`Total_Time_sec\`,
    SUM_ROWS_EXAMINED AS \`Rows_Examined\`,
    SUM_ROWS_SENT AS \`Rows_Sent\`,
    ROUND(SUM_ROWS_EXAMINED / NULLIF(SUM_ROWS_SENT, 0), 0) AS \`Exam_Sent_Ratio\`,
    SUM_NO_INDEX_USED AS \`No_Index_Count\`
  FROM performance_schema.events_statements_summary_by_digest
  WHERE SCHEMA_NAME = '$escaped_db'
    AND SUM_NO_INDEX_USED > 0
  ORDER BY SUM_ROWS_EXAMINED DESC
  LIMIT 20;
  ")
  tsv_to_md "$RESULT"
  echo ""

  # 5-3. Queries causing temp disk tables / sort merge passes
  echo "#### Queries Causing Disk Temp Tables / Sort Merge Passes"
  echo ""
  RESULT=$(run_sql_with_headers "
  SELECT
    LEFT(DIGEST_TEXT, 200) AS \`Query_Digest\`,
    COUNT_STAR AS \`Exec_Count\`,
    ROUND(SUM_TIMER_WAIT/1000000000000, 2) AS \`Total_Time_sec\`,
    SUM_CREATED_TMP_DISK_TABLES AS \`Tmp_Disk_Tables\`,
    SUM_SORT_MERGE_PASSES AS \`Sort_Merge_Passes\`,
    SUM_SORT_ROWS AS \`Sort_Rows\`
  FROM performance_schema.events_statements_summary_by_digest
  WHERE SCHEMA_NAME = '$escaped_db'
    AND (SUM_CREATED_TMP_DISK_TABLES > 0 OR SUM_SORT_MERGE_PASSES > 0)
  ORDER BY SUM_CREATED_TMP_DISK_TABLES + SUM_SORT_MERGE_PASSES DESC
  LIMIT 15;
  ")
  tsv_to_md "$RESULT"
  echo ""

  # 5-4. Highest total time queries
  echo "#### Highest Total Time Queries"
  echo ""
  RESULT=$(run_sql_with_headers "
  SELECT
    LEFT(DIGEST_TEXT, 200) AS \`Query_Digest\`,
    COUNT_STAR AS \`Exec_Count\`,
    ROUND(SUM_TIMER_WAIT/1000000000000, 2) AS \`Total_Time_sec\`,
    ROUND(AVG_TIMER_WAIT/1000000000000, 4) AS \`Avg_Time_sec\`,
    SUM_ROWS_EXAMINED AS \`Rows_Examined\`,
    SUM_NO_INDEX_USED AS \`No_Index_Used\`
  FROM performance_schema.events_statements_summary_by_digest
  WHERE SCHEMA_NAME = '$escaped_db'
  ORDER BY SUM_TIMER_WAIT DESC
  LIMIT 15;
  ")
  tsv_to_md "$RESULT"
  echo ""
}

if [[ $HAS_PERF_SCHEMA -eq 1 ]]; then
  step "Analyzing queries..."
  echo "## 5. Query Analysis"
  echo ""

  if [[ -n "$TARGET_DB" ]]; then
    analyze_queries_for_db "$TARGET_DB"
  else
    # Only analyze DBs with significant query load (>100 executions)
    ACTIVE_DBS=$(run_sql "
    SELECT SCHEMA_NAME
    FROM performance_schema.events_statements_summary_by_digest
    WHERE SCHEMA_NAME IS NOT NULL
      AND SCHEMA_NAME NOT IN ($SYSTEM_DBS)
    GROUP BY SCHEMA_NAME
    HAVING SUM(COUNT_STAR) > 100
    ORDER BY SUM(SUM_TIMER_WAIT) DESC;
    ")
    while IFS= read -r db; do
      [[ -z "$db" ]] && continue
      analyze_queries_for_db "$db"
    done <<< "$ACTIVE_DBS"
  fi

  ok "Query analysis done"
fi


###############################################################################
# Section 6: Table Structure Analysis
###############################################################################

analyze_structure_for_db() {
  local db="$1"
  local escaped_db
  escaped_db=$(echo "$db" | sed "s/'/\\\\'/g")

  echo "### Structure: \`$db\`"
  echo ""

  # 6-1. Large text columns (longtext, mediumtext, text, blob)
  echo "#### Large Text/Blob Columns"
  echo ""
  RESULT=$(run_sql_with_headers "
  SELECT
    TABLE_NAME AS \`Table\`,
    COLUMN_NAME AS \`Column\`,
    DATA_TYPE AS \`Type\`,
    ROUND(
      (SELECT DATA_LENGTH FROM information_schema.TABLES t
       WHERE t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME
      ) / 1024 / 1024, 2
    ) AS \`Table_Data_MB\`
  FROM information_schema.COLUMNS c
  WHERE TABLE_SCHEMA = '$escaped_db'
    AND DATA_TYPE IN ('longtext','mediumtext','longblob','mediumblob')
  ORDER BY TABLE_NAME, ORDINAL_POSITION;
  ")
  local lob_count
  lob_count=$(echo "$RESULT" | tail -n +2 | awk 'NF{c++}END{print c+0}')
  if [[ "$lob_count" -gt 0 ]]; then
    echo "> Tables with longtext/mediumtext columns may suffer from I/O amplification during full scans."
    echo ""
    tsv_to_md "$RESULT"
  else
    echo "(none)"
  fi
  echo ""

  # 6-2. ENUM columns with many values
  echo "#### ENUM Columns"
  echo ""
  RESULT=$(run_sql_with_headers "
  SELECT
    TABLE_NAME AS \`Table\`,
    COLUMN_NAME AS \`Column\`,
    LENGTH(COLUMN_TYPE) - LENGTH(REPLACE(COLUMN_TYPE, ',', '')) + 1 AS \`Enum_Values\`
  FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA = '$escaped_db'
    AND DATA_TYPE = 'enum'
  HAVING \`Enum_Values\` >= 5
  ORDER BY \`Enum_Values\` DESC;
  ")
  local enum_count
  enum_count=$(echo "$RESULT" | tail -n +2 | awk 'NF{c++}END{print c+0}')
  if [[ "$enum_count" -gt 0 ]]; then
    echo "> ENUM columns with many values: ALTER TABLE to add values requires table rebuild → MDL lock risk."
    echo ""
    tsv_to_md "$RESULT"
  else
    echo "(none with 5+ values)"
  fi
  echo ""

  # 6-3. Tables without primary key
  echo "#### Tables Without Primary Key"
  echo ""
  RESULT=$(run_sql_with_headers "
  SELECT t.TABLE_NAME AS \`Table\`, t.TABLE_ROWS AS \`Rows\`
  FROM information_schema.TABLES t
  LEFT JOIN information_schema.TABLE_CONSTRAINTS tc
    ON t.TABLE_SCHEMA = tc.TABLE_SCHEMA
    AND t.TABLE_NAME = tc.TABLE_NAME
    AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
  WHERE t.TABLE_SCHEMA = '$escaped_db'
    AND t.TABLE_TYPE = 'BASE TABLE'
    AND tc.CONSTRAINT_NAME IS NULL
  ORDER BY t.TABLE_ROWS DESC;
  ")
  local nopk_count
  nopk_count=$(echo "$RESULT" | tail -n +2 | awk 'NF{c++}END{print c+0}')
  if [[ "$nopk_count" -gt 0 ]]; then
    echo "> Tables without PK have poor InnoDB performance and replication issues."
    echo ""
    tsv_to_md "$RESULT"
  else
    echo "(all tables have primary keys)"
  fi
  echo ""

  # 6-4. Oversized rows (table data_mb / rows → avg row size)
  echo "#### Tables with Large Average Row Size (>1KB)"
  echo ""
  RESULT=$(run_sql_with_headers "
  SELECT
    TABLE_NAME AS \`Table\`,
    TABLE_ROWS AS \`Rows\`,
    ROUND(DATA_LENGTH/1024/1024, 2) AS \`Data_MB\`,
    ROUND(AVG_ROW_LENGTH/1024, 2) AS \`Avg_Row_KB\`
  FROM information_schema.TABLES
  WHERE TABLE_SCHEMA = '$escaped_db'
    AND TABLE_TYPE = 'BASE TABLE'
    AND AVG_ROW_LENGTH > 1024
    AND TABLE_ROWS > 0
  ORDER BY AVG_ROW_LENGTH DESC
  LIMIT 15;
  ")
  local bigrow_count
  bigrow_count=$(echo "$RESULT" | tail -n +2 | awk 'NF{c++}END{print c+0}')
  if [[ "$bigrow_count" -gt 0 ]]; then
    tsv_to_md "$RESULT"
  else
    echo "(none)"
  fi
  echo ""
}

step "Analyzing table structures..."
echo "## 6. Table Structure Analysis"
echo ""

if [[ -n "$TARGET_DB" ]]; then
  analyze_structure_for_db "$TARGET_DB"
else
  SIZED_DBS=$(run_sql "
  SELECT TABLE_SCHEMA
  FROM information_schema.TABLES
  WHERE TABLE_SCHEMA NOT IN ($SYSTEM_DBS)
  GROUP BY TABLE_SCHEMA
  HAVING SUM(DATA_LENGTH + INDEX_LENGTH) > 1048576
  ORDER BY SUM(DATA_LENGTH + INDEX_LENGTH) DESC;
  ")
  while IFS= read -r db; do
    [[ -z "$db" ]] && continue
    analyze_structure_for_db "$db"
  done <<< "$SIZED_DBS"
fi

ok "Table structure analysis done"


###############################################################################
# Section 7: Lock & Wait Analysis
###############################################################################

if [[ $HAS_PERF_SCHEMA -eq 1 ]]; then
  step "Analyzing locks and waits..."
  echo "## 7. Lock & Wait Analysis"
  echo ""

  # 7-1. Lock wait events
  echo "### 7-1. Lock Wait Events"
  echo ""
  RESULT=$(run_sql_with_headers "
  SELECT
    EVENT_NAME AS \`Event\`,
    COUNT_STAR AS \`Count\`,
    ROUND(SUM_TIMER_WAIT/1000000000000, 4) AS \`Total_Wait_sec\`,
    ROUND(AVG_TIMER_WAIT/1000000000000, 6) AS \`Avg_Wait_sec\`,
    ROUND(MAX_TIMER_WAIT/1000000000000, 4) AS \`Max_Wait_sec\`
  FROM performance_schema.events_waits_summary_global_by_event_name
  WHERE EVENT_NAME LIKE '%lock%' AND COUNT_STAR > 0
  ORDER BY SUM_TIMER_WAIT DESC
  LIMIT 15;
  ")
  tsv_to_md "$RESULT"
  echo ""

  # 7-2. I/O wait events
  echo "### 7-2. I/O Wait Events (Top 15)"
  echo ""
  RESULT=$(run_sql_with_headers "
  SELECT
    EVENT_NAME AS \`Event\`,
    COUNT_STAR AS \`Count\`,
    ROUND(SUM_TIMER_WAIT/1000000000000, 4) AS \`Total_Wait_sec\`,
    ROUND(AVG_TIMER_WAIT/1000000000000, 6) AS \`Avg_Wait_sec\`,
    ROUND(MAX_TIMER_WAIT/1000000000000, 4) AS \`Max_Wait_sec\`
  FROM performance_schema.events_waits_summary_global_by_event_name
  WHERE EVENT_NAME LIKE 'wait/io/%' AND COUNT_STAR > 0
  ORDER BY SUM_TIMER_WAIT DESC
  LIMIT 15;
  ")
  tsv_to_md "$RESULT"
  echo ""

  # 7-3. File I/O summary
  echo "### 7-3. File I/O Summary (Top 15)"
  echo ""
  RESULT=$(run_sql_with_headers "
  SELECT
    FILE_NAME AS \`File\`,
    COUNT_READ AS \`Reads\`,
    ROUND(SUM_TIMER_READ/1000000000000, 4) AS \`Read_Time_sec\`,
    COUNT_WRITE AS \`Writes\`,
    ROUND(SUM_TIMER_WRITE/1000000000000, 4) AS \`Write_Time_sec\`
  FROM performance_schema.file_summary_by_instance
  ORDER BY SUM_TIMER_WAIT DESC
  LIMIT 15;
  ")
  tsv_to_md "$RESULT"
  echo ""

  # 7-4. Current MDL locks (if any)
  echo "### 7-4. Current Metadata Locks"
  echo ""
  RESULT=$(run_sql_with_headers "
  SELECT
    OBJECT_TYPE AS \`Type\`,
    OBJECT_SCHEMA AS \`Database\`,
    OBJECT_NAME AS \`Object\`,
    LOCK_TYPE AS \`Lock_Type\`,
    LOCK_DURATION AS \`Duration\`,
    LOCK_STATUS AS \`Status\`,
    OWNER_THREAD_ID AS \`Thread_ID\`
  FROM performance_schema.metadata_locks
  WHERE OWNER_THREAD_ID != 0
    AND OBJECT_SCHEMA NOT IN ($SYSTEM_DBS)
  LIMIT 20;
  ")
  mdl_count=$(echo "$RESULT" | tail -n +2 | awk 'NF{c++}END{print c+0}')
  if [[ "$mdl_count" -gt 0 ]]; then
    tsv_to_md "$RESULT"
  else
    echo "(no active metadata locks)"
  fi
  echo ""

  # 7-5. DDL statements (ALTER TABLE etc.)
  echo "### 7-5. Recent DDL Statements"
  echo ""
  RESULT=$(run_sql_with_headers "
  SELECT
    SCHEMA_NAME AS \`Database\`,
    LEFT(DIGEST_TEXT, 200) AS \`DDL_Statement\`,
    COUNT_STAR AS \`Count\`,
    ROUND(SUM_TIMER_WAIT/1000000000000, 2) AS \`Total_Time_sec\`,
    ROUND(MAX_TIMER_WAIT/1000000000000, 2) AS \`Max_Time_sec\`,
    FIRST_SEEN AS \`First_Seen\`,
    LAST_SEEN AS \`Last_Seen\`
  FROM performance_schema.events_statements_summary_by_digest
  WHERE DIGEST_TEXT LIKE 'ALTER%'
    OR DIGEST_TEXT LIKE 'CREATE INDEX%'
    OR DIGEST_TEXT LIKE 'DROP INDEX%'
  ORDER BY LAST_SEEN DESC
  LIMIT 15;
  ")
  ddl_count=$(echo "$RESULT" | tail -n +2 | awk 'NF{c++}END{print c+0}')
  if [[ "$ddl_count" -gt 0 ]]; then
    echo "> DDL on large tables can cause MDL lock contention."
    echo ""
    tsv_to_md "$RESULT"
  else
    echo "(no DDL statements found)"
  fi
  echo ""

  ok "Lock & wait analysis done"
fi


###############################################################################
# Section 8: Server Variable Tuning Recommendations
###############################################################################
step "Generating tuning recommendations..."
echo "## 8. Server Variable Tuning Recommendations"
echo ""

# Collect values for analysis
SORT_BUF=$(run_sql "SELECT @@sort_buffer_size;" 2>/dev/null || echo "0")
JOIN_BUF=$(run_sql "SELECT @@join_buffer_size;" 2>/dev/null || echo "0")
TMP_SIZE=$(run_sql "SELECT @@tmp_table_size;" 2>/dev/null || echo "0")
HEAP_SIZE=$(run_sql "SELECT @@max_heap_table_size;" 2>/dev/null || echo "0")
SLOW_LOG_VAR=$(run_sql "SELECT @@slow_query_log;" 2>/dev/null || echo "0")
LONG_QT=$(run_sql "SELECT @@long_query_time;" 2>/dev/null || echo "0")
LOG_NO_IDX=$(run_sql "SELECT @@log_queries_not_using_indexes;" 2>/dev/null || echo "0")
IO_CAP=$(run_sql "SELECT @@innodb_io_capacity;" 2>/dev/null || echo "0")
AHI=$(run_sql "SELECT @@innodb_adaptive_hash_index;" 2>/dev/null || echo "0")
READ_THREADS=$(run_sql "SELECT @@innodb_read_io_threads;" 2>/dev/null || echo "0")

SORT_MERGE=$(run_sql "SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Sort_merge_passes';" 2>/dev/null || echo "0")
FULL_JOIN=$(run_sql "SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Select_full_join';" 2>/dev/null || echo "0")
TMP_DISK=$(run_sql "SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Created_tmp_disk_tables';" 2>/dev/null || echo "0")
TMP_TOTAL=$(run_sql "SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Created_tmp_tables';" 2>/dev/null || echo "0")
SLOW_Q=$(run_sql "SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Slow_queries';" 2>/dev/null || echo "0")

echo "| Variable | Current | Recommendation | Reason |"
echo "| --- | --- | --- | --- |"

# sort_buffer_size
if [[ "$SORT_MERGE" -gt 1000 ]]; then
  SORT_BUF_MB=$(awk "BEGIN {printf \"%.0f\", $SORT_BUF / 1024 / 1024}")
  echo "| sort_buffer_size | ${SORT_BUF_MB}MB | 2MB | Sort_merge_passes=$SORT_MERGE — frequent disk sorts |"
fi

# join_buffer_size
if [[ "$FULL_JOIN" -gt 10000 ]]; then
  JOIN_BUF_MB=$(awk "BEGIN {printf \"%.0f\", $JOIN_BUF / 1024 / 1024}")
  echo "| join_buffer_size | ${JOIN_BUF_MB}MB | 1MB | Select_full_join=$FULL_JOIN — joins without indexes |"
fi

# tmp_table_size / max_heap_table_size
if [[ "$TMP_DISK" -gt 100 ]]; then
  TMP_MB=$(awk "BEGIN {printf \"%.0f\", $TMP_SIZE / 1024 / 1024}")
  HEAP_MB=$(awk "BEGIN {printf \"%.0f\", $HEAP_SIZE / 1024 / 1024}")
  echo "| tmp_table_size | ${TMP_MB}MB | 64MB | Created_tmp_disk_tables=$TMP_DISK |"
  if [[ "$TMP_SIZE" != "$HEAP_SIZE" ]]; then
    echo "| max_heap_table_size | ${HEAP_MB}MB | 64MB | Should match tmp_table_size |"
  fi
fi

# slow_query_log
if [[ "$SLOW_LOG_VAR" == "0" ]]; then
  echo "| slow_query_log | OFF | ON | Cannot track slow queries without this |"
fi

# long_query_time
LONG_QT_INT=$(echo "$LONG_QT" | cut -d. -f1)
if [[ "$LONG_QT_INT" -gt 5 ]]; then
  echo "| long_query_time | ${LONG_QT}s | 3s | ${LONG_QT}s is too lenient |"
fi

# log_queries_not_using_indexes
if [[ "$LOG_NO_IDX" == "0" ]]; then
  echo "| log_queries_not_using_indexes | OFF | ON | Track full-scan queries |"
fi

# innodb_io_capacity
if [[ "$IO_CAP" -lt 1000 ]]; then
  echo "| innodb_io_capacity | $IO_CAP | 2000-4000 | Low for SSD storage (review if SSD) |"
fi

# innodb_adaptive_hash_index
if [[ "$AHI" == "0" || "$AHI" == "OFF" ]]; then
  echo "| innodb_adaptive_hash_index | OFF | ON (test) | May help with repeated point lookups |"
fi

# innodb_read_io_threads
if [[ "$READ_THREADS" -lt 8 ]]; then
  echo "| innodb_read_io_threads | $READ_THREADS | 8-16 | High read workload may benefit |"
fi

echo ""
echo "> Recommendations are based on current global status counters. Test changes in staging first."
echo ""

ok "Tuning recommendations done"


###############################################################################
# Section 9: InnoDB Status Summary
###############################################################################
step "Collecting InnoDB status..."
echo "## 9. InnoDB Status Summary"
echo ""

INNODB_STATUS=$(run_sql "SHOW ENGINE INNODB STATUS;" 2>/dev/null || echo "")
if [[ -n "$INNODB_STATUS" ]]; then
  # Extract key sections
  echo "### Latest Deadlock"
  echo ""
  DEADLOCK=$(echo "$INNODB_STATUS" | sed -n '/LATEST DETECTED DEADLOCK/,/^---/p' | head -30)
  if [[ -n "$DEADLOCK" && ! "$DEADLOCK" =~ "WE ROLL BACK" || ${#DEADLOCK} -gt 10 ]]; then
    echo '```'
    echo "$DEADLOCK"
    echo '```'
  else
    echo "(no deadlock recorded)"
  fi
  echo ""

  echo "### Latest Foreign Key Error"
  echo ""
  FK_ERR=$(echo "$INNODB_STATUS" | sed -n '/LATEST FOREIGN KEY ERROR/,/^---/p' | head -20)
  if [[ -n "$FK_ERR" && ${#FK_ERR} -gt 10 ]]; then
    echo '```'
    echo "$FK_ERR"
    echo '```'
  else
    echo "(no FK error recorded)"
  fi
  echo ""

  echo "### Buffer Pool Summary"
  echo ""
  BP_SUMMARY=$(echo "$INNODB_STATUS" | sed -n '/BUFFER POOL AND MEMORY/,/^---/p' | head -20)
  if [[ -n "$BP_SUMMARY" ]]; then
    echo '```'
    echo "$BP_SUMMARY"
    echo '```'
  fi
  echo ""

  echo "### Row Operations"
  echo ""
  ROW_OPS=$(echo "$INNODB_STATUS" | sed -n '/ROW OPERATIONS/,/^---/p' | head -10)
  if [[ -n "$ROW_OPS" ]]; then
    echo '```'
    echo "$ROW_OPS"
    echo '```'
  fi
  echo ""
else
  echo "(SHOW ENGINE INNODB STATUS not available or no permission)"
  echo ""
fi

ok "InnoDB status done"

###############################################################################
# Section 10: Inactive Databases
###############################################################################
step "Checking for inactive databases..."
echo "## 10. Inactive / Low-Activity Databases"
echo ""
echo "> Databases with no query activity or minimal data. Consider backup + DROP."
echo ""

if [[ $HAS_PERF_SCHEMA -eq 1 ]]; then
  RESULT=$(run_sql_with_headers "
  SELECT
    s.SCHEMA_NAME AS \`Database\`,
    ROUND(COALESCE(SUM(t.DATA_LENGTH + t.INDEX_LENGTH), 0)/1024/1024, 2) AS \`Size_MB\`,
    COALESCE(q.total_exec, 0) AS \`Total_Queries\`,
    COALESCE(q.total_time, 0) AS \`Total_Time_sec\`
  FROM information_schema.SCHEMATA s
  LEFT JOIN information_schema.TABLES t
    ON s.SCHEMA_NAME = t.TABLE_SCHEMA
  LEFT JOIN (
    SELECT
      SCHEMA_NAME,
      SUM(COUNT_STAR) AS total_exec,
      ROUND(SUM(SUM_TIMER_WAIT)/1000000000000, 2) AS total_time
    FROM performance_schema.events_statements_summary_by_digest
    GROUP BY SCHEMA_NAME
  ) q ON s.SCHEMA_NAME = q.SCHEMA_NAME
  WHERE s.SCHEMA_NAME NOT IN ($SYSTEM_DBS)
  GROUP BY s.SCHEMA_NAME, q.total_exec, q.total_time
  HAVING COALESCE(q.total_exec, 0) < 100
  ORDER BY COALESCE(q.total_exec, 0) ASC, Size_MB DESC;
  ")
  inactive_count=$(echo "$RESULT" | tail -n +2 | awk 'NF{c++}END{print c+0}')
  if [[ "$inactive_count" -gt 0 ]]; then
    tsv_to_md "$RESULT"
  else
    echo "(all databases have significant activity)"
  fi
else
  echo "(requires performance_schema to detect)"
fi
echo ""

ok "Inactive database check done"

###############################################################################
# Footer
###############################################################################
echo "---"
echo ""
echo "> Analysis complete. $(date '+%Y-%m-%d %H:%M:%S %Z')"

} > "$OUTPUT"

echo ""
echo -e "${GREEN}═══════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  Analysis complete!${NC}"
echo -e "${GREEN}  Report saved to: ${OUTPUT}${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════════${NC}"

