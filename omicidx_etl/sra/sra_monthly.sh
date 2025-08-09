#!/bin/bash
set -e

# SRA Monthly Extraction Script
# Optimized for local server deployment (512GB RAM, 64 cores)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
OUTPUT_DIR="/data/sra/$(date +%Y-%m)"
LOG_FILE="/var/log/omicidx/sra-extract-$(date +%Y%m%d).log"

# Configuration
MAX_WORKERS=8  # Conservative for SRA's large files
OUTPUT_FORMAT="parquet"  # Default to Parquet for DuckDB
UPLOAD_TO_R2=true
INCLUDE_ACCESSIONS=true  # Include SRA accessions extraction

echo "SRA ETL Starting at $(date)" | tee -a "$LOG_FILE"
echo "Output Directory: $OUTPUT_DIR" | tee -a "$LOG_FILE"
echo "Workers: $MAX_WORKERS" | tee -a "$LOG_FILE"
echo "Format: $OUTPUT_FORMAT" | tee -a "$LOG_FILE"
echo "Include Accessions: $INCLUDE_ACCESSIONS" | tee -a "$LOG_FILE"

# Create output directory
mkdir -p "$OUTPUT_DIR"
mkdir -p "$(dirname "$LOG_FILE")"

# Change to project directory
cd "$PROJECT_ROOT"

# Activate environment and run extraction
ACCESSIONS_FLAG=""
if [ "$INCLUDE_ACCESSIONS" = true ]; then
    ACCESSIONS_FLAG="--include-accessions"
else
    ACCESSIONS_FLAG="--no-accessions"
fi

if [ "$UPLOAD_TO_R2" = true ]; then
    echo "Running SRA extraction with R2 upload..." | tee -a "$LOG_FILE"
    uv run omicidx-etl sra extract "$OUTPUT_DIR" \
        --format "$OUTPUT_FORMAT" \
        --workers "$MAX_WORKERS" \
        --upload \
        $ACCESSIONS_FLAG 2>&1 | tee -a "$LOG_FILE"
else
    echo "Running SRA extraction (local only)..." | tee -a "$LOG_FILE"
    uv run omicidx-etl sra extract "$OUTPUT_DIR" \
        --format "$OUTPUT_FORMAT" \
        --workers "$MAX_WORKERS" \
        --no-upload \
        $ACCESSIONS_FLAG 2>&1 | tee -a "$LOG_FILE"
fi

# Show statistics
echo "Extraction completed. Generating statistics..." | tee -a "$LOG_FILE"
uv run omicidx-etl sra stats "$OUTPUT_DIR" --format "$OUTPUT_FORMAT" 2>&1 | tee -a "$LOG_FILE"

# Cleanup old extractions (keep last 3 months)
echo "Cleaning up old extractions..." | tee -a "$LOG_FILE"
find "/data/sra" -maxdepth 1 -type d -name "20*" | sort | head -n -3 | while read -r old_dir; do
    if [ -d "$old_dir" ]; then
        echo "Removing old extraction: $old_dir" | tee -a "$LOG_FILE"
        rm -rf "$old_dir"
    fi
done

echo "SRA ETL completed at $(date)" | tee -a "$LOG_FILE"

# Optional: Send completion notification
# curl -X POST "https://your-webhook-url" -d "SRA extraction completed: $OUTPUT_DIR"
