#!/bin/bash
#
# Enhanced OmicIDX Daily Pipeline
#
# This script orchestrates the daily ETL pipeline with proper logging,
# error handling, and metrics collection.
#
# Features:
# - Structured logging to files
# - JSON metrics tracking per task
# - Duration tracking
# - Error handling with continue-on-error
# - Summary reporting

set -euo pipefail

# Configuration
EXTRACT_DIR="${EXTRACT_DIR:-/data/davsean/omicidx_root}"
LOG_DIR="${LOG_DIR:-${EXTRACT_DIR}/logs}"
METRICS_DIR="${EXTRACT_DIR}/metrics"
RUN_ID=$(date +%Y%m%d_%H%M%S)
LOG_FILE="${LOG_DIR}/run_${RUN_ID}.log"
METRICS_FILE="${METRICS_DIR}/run_${RUN_ID}.json"

# Ensure directories exist
mkdir -p "$LOG_DIR" "$METRICS_DIR"

# Initialize metrics JSON
cat > "$METRICS_FILE" <<EOF
{
  "run_id": "$RUN_ID",
  "start_time": "$(date -Iseconds)",
  "tasks": {}
}
EOF

# Logging function
log() {
    local level="$1"
    shift
    local message="$*"
    local timestamp=$(date -Iseconds)
    echo "[$timestamp] [$level] $message" | tee -a "$LOG_FILE"
}

# Update metrics JSON for a task
update_metrics() {
    local task_name="$1"
    local status="$2"
    local duration="$3"
    local error="${4:-}"

    local tmp_file="${METRICS_FILE}.tmp"

    if [ -n "$error" ]; then
        jq ".tasks[\"$task_name\"] = {status: \"$status\", duration: $duration, error: \"$error\"}" \
            "$METRICS_FILE" > "$tmp_file"
    else
        jq ".tasks[\"$task_name\"] = {status: \"$status\", duration: $duration}" \
            "$METRICS_FILE" > "$tmp_file"
    fi

    mv "$tmp_file" "$METRICS_FILE"
}

# Run a task with timing and error handling
run_task() {
    local task_name="$1"
    shift
    local cmd=("$@")

    log "INFO" "Starting task: $task_name"
    local start=$(date +%s)

    # Run command and capture output
    if "${cmd[@]}" 2>&1 | tee -a "$LOG_FILE"; then
        local end=$(date +%s)
        local duration=$((end - start))
        update_metrics "$task_name" "success" "$duration"
        log "INFO" "✓ Task completed: $task_name (${duration}s)"
        return 0
    else
        local end=$(date +%s)
        local duration=$((end - start))
        local exit_code=$?
        update_metrics "$task_name" "failed" "$duration" "Exit code: $exit_code"
        log "ERROR" "✗ Task failed: $task_name (${duration}s, exit code: $exit_code)"
        return 1
    fi
}

# Main pipeline execution
main() {
    log "INFO" "=========================================="
    log "INFO" "OmicIDX Daily Pipeline"
    log "INFO" "=========================================="
    log "INFO" "Run ID: $RUN_ID"
    log "INFO" "Extract directory: $EXTRACT_DIR"
    log "INFO" "Log file: $LOG_FILE"
    log "INFO" "Metrics file: $METRICS_FILE"
    log "INFO" "=========================================="

    # Track failed tasks
    local failed_tasks=()

    # Stage 1: Data Extraction
    log "INFO" "\nStage 1: Data Extraction"
    log "INFO" "------------------------"

    run_task "extract_sra" uv run oidx sra extract "$EXTRACT_DIR/sra" \
        || failed_tasks+=("extract_sra")

    run_task "extract_ebi_biosample" uv run oidx ebi-biosample extract --output-dir "$EXTRACT_DIR/ebi_biosample" \
        || failed_tasks+=("extract_ebi_biosample")

    run_task "extract_biosample" uv run oidx biosample extract "$EXTRACT_DIR/biosample" \
        || failed_tasks+=("extract_biosample")

    run_task "extract_europepmc" uv run oidx europepmc extract "$EXTRACT_DIR/europepmc" \
        || failed_tasks+=("extract_europepmc")

    run_task "extract_geo" uv run oidx geo extract \
        || failed_tasks+=("extract_geo")

    # Stage 2: Transformations
    # Only run if at least some extractions succeeded
    if [ ${#failed_tasks[@]} -lt 5 ]; then
        log "INFO" "\nStage 2: Transformations"
        log "INFO" "------------------------"

        run_task "transform_data" uv run oidx transform run --extract-dir "$EXTRACT_DIR" \
            || failed_tasks+=("transform_data")
    else
        log "WARN" "Skipping transformations - too many extraction failures"
    fi

    # Stage 3: Catalog
    log "INFO" "\nStage 3: Metadata Catalog"
    log "INFO" "-------------------------"

    run_task "build_catalog" uv run oidx catalog build --data-dir "$EXTRACT_DIR" \
        || failed_tasks+=("build_catalog")

    # Finalize metrics
    local tmp_file="${METRICS_FILE}.tmp"
    jq ".end_time = \"$(date -Iseconds)\"" "$METRICS_FILE" > "$tmp_file"
    mv "$tmp_file" "$METRICS_FILE"

    # Summary
    log "INFO" "\n=========================================="
    log "INFO" "Pipeline Summary"
    log "INFO" "=========================================="
    log "INFO" "Run ID: $RUN_ID"
    log "INFO" "Total tasks: $(jq '.tasks | length' "$METRICS_FILE")"
    log "INFO" "Successful: $(jq '[.tasks[] | select(.status == "success")] | length' "$METRICS_FILE")"
    log "INFO" "Failed: ${#failed_tasks[@]}"

    if [ ${#failed_tasks[@]} -gt 0 ]; then
        log "ERROR" "Failed tasks: ${failed_tasks[*]}"
    fi

    log "INFO" "Metrics: $METRICS_FILE"
    log "INFO" "Logs: $LOG_FILE"
    log "INFO" "=========================================="

    # Exit with error if any tasks failed
    if [ ${#failed_tasks[@]} -gt 0 ]; then
        log "ERROR" "Pipeline completed with errors"
        return 1
    else
        log "INFO" "✓ Pipeline completed successfully"
        return 0
    fi
}

# Run main pipeline
main
exit $?
