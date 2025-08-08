#!/bin/bash
# Monthly biosample/bioproject extraction script
# Add to crontab: 0 2 1 * * /path/to/scripts/biosample_monthly.sh

set -e  # Exit on any error

# Configuration
PROJECT_DIR="/path/to/omicidx-gh-etl"
OUTPUT_DIR="/data/omicidx/biosample"
VENV_DIR="$PROJECT_DIR/.venv"
LOG_FILE="/var/log/omicidx/biosample-$(date +%Y%m%d).log"

# Logging function
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

# Create log directory
mkdir -p "$(dirname "$LOG_FILE")"

log "Starting monthly biosample/bioproject extraction"

# Change to project directory
cd "$PROJECT_DIR"

# Activate virtual environment
if [ -f "$VENV_DIR/bin/activate" ]; then
    source "$VENV_DIR/bin/activate"
    log "Activated virtual environment"
else
    log "ERROR: Virtual environment not found at $VENV_DIR"
    exit 1
fi

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Run extraction
log "Starting extraction to $OUTPUT_DIR"
python -m omicidx_etl.cli biosample extract \
    --output-dir "$OUTPUT_DIR" \
    --entity both \
    --upload 2>&1 | tee -a "$LOG_FILE"

# Check if extraction was successful
if [ $? -eq 0 ]; then
    log "✓ Extraction completed successfully"
    
    # Show statistics
    log "File statistics:"
    python -m omicidx_etl.cli biosample stats \
        --output-dir "$OUTPUT_DIR" 2>&1 | tee -a "$LOG_FILE"
    
    # Optional: Create DuckDB analytics database
    if command -v duckdb &> /dev/null; then
        log "Creating DuckDB analytics database"
        duckdb "$OUTPUT_DIR/biosample.duckdb" << EOF
-- Load latest data
CREATE OR REPLACE TABLE biosample_latest AS
SELECT * FROM read_ndjson('$OUTPUT_DIR/biosample-*.ndjson.gz');

CREATE OR REPLACE TABLE bioproject_latest AS  
SELECT * FROM read_ndjson('$OUTPUT_DIR/bioproject-*.ndjson.gz');

-- Create some basic analytics views
CREATE OR REPLACE VIEW monthly_stats AS
SELECT 
    'biosample' as entity_type,
    COUNT(*) as total_records,
    MIN(last_update_date) as earliest_update,
    MAX(last_update_date) as latest_update
FROM biosample_latest
UNION ALL
SELECT 
    'bioproject' as entity_type,
    COUNT(*) as total_records,
    MIN(last_update_date) as earliest_update,
    MAX(last_update_date) as latest_update
FROM bioproject_latest;

-- Show summary
SELECT * FROM monthly_stats;
EOF
        log "✓ DuckDB analytics database updated"
    fi
    
else
    log "✗ Extraction failed"
    exit 1
fi

# Optional: Cleanup old log files (keep last 30 days)
find "$(dirname "$LOG_FILE")" -name "biosample-*.log" -type f -mtime +30 -delete 2>/dev/null || true

log "Monthly extraction completed"
