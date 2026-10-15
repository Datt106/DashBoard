#!/bin/bash
# Run Kimball v3 cho 10 ngày, mỗi ngày 1 folder riêng
set -e
cd ~/bigdata_project

LOG_DIR=/home/datnt/bigdata_project/output
mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/run_10days.log"
: > "$LOG"

DATES=(
  2026-09-02 2026-09-03 2026-09-04 2026-09-05 2026-09-06
  2026-09-07 2026-09-08 2026-09-09 2026-09-10 2026-09-11
)

echo "=============================================" | tee -a "$LOG"
echo "START: $(date)  PIDs spark sẽ tự thoát sau mỗi ngày" | tee -a "$LOG"
echo "=============================================" | tee -a "$LOG"

for d in "${DATES[@]}"; do
    echo "" | tee -a "$LOG"
    echo "=== Date: $d  $(date) ===" | tee -a "$LOG"
    
    MOBILE_PATH="hdfs://10.5.94.214:19000/data_ec/ad-pageview-mobile/parquet/$d"
    PC_PATH="hdfs://10.5.94.214:19000/data_ec/ad-pageview-pc/parquet/$d"
    
    if ! hdfs dfs -test -d "$MOBILE_PATH"; then
        echo "SKIP: missing $MOBILE_PATH" | tee -a "$LOG"
        continue
    fi
    if ! hdfs dfs -test -d "$PC_PATH"; then
        echo "SKIP: missing $PC_PATH" | tee -a "$LOG"
        continue
    fi
    
    GOLD_DAY="/data/softs/datnt_workspace/output/gold/$d"
    LOC_DAY="/home/datnt/bigdata_project/output/$d"
    mkdir -p "$GOLD_DAY" "$LOC_DAY"
    
    export GOLD_BASE="$GOLD_DAY"
    export TARGET_DATE="$d"
    export LOCAL_OUTPUT="$LOC_DAY"
    
    echo "  GOLD_BASE=$GOLD_BASE" | tee -a "$LOG"
    echo "  LOCAL_OUTPUT=$LOCAL_OUTPUT" | tee -a "$LOG"
    
    if SPARK_LOCAL_IP=10.5.94.214 spark-submit \
            --master spark://10.5.94.214:17077 \
            build_kimball_v3.py >> "$LOG" 2>&1; then
        echo "  OK: $d done at $(date)" | tee -a "$LOG"
    else
        echo "  FAIL: $d at $(date) - xem log để biết chi tiết" | tee -a "$LOG"
    fi
done

echo "" | tee -a "$LOG"
echo "=============================================" | tee -a "$LOG"
echo "ALL DONE: $(date)" | tee -a "$LOG"
echo "=============================================" | tee -a "$LOG"
