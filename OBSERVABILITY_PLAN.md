# Observability & Orchestration Implementation Plan

## Overview

Enhance the existing omicidx-etl pipeline with observability, error handling, and transformation capabilities while maintaining simplicity and avoiding heavy orchestration frameworks.

## Goals

1. ✅ **Observability**: Track pipeline runs, task durations, success/failure rates
2. ✅ **Error Handling**: Proper retry logic, failure tracking, and logging
3. ✅ **Transformations**: Add DuckDB transformation and cataloging steps
4. ✅ **Status Dashboard**: Simple CLI tool to view pipeline health
5. ✅ **Maintainability**: Keep it simple for handoff - no Prefect/Airflow complexity

## Architecture

```
daily_pipeline.sh (orchestrator)
    ↓
    ├─ oidx extract commands (existing CLI)
    ├─ oidx transform run (new: DuckDB transformations)
    └─ Metrics & Logging (JSON + structured logs)

oidx status (new: view dashboard)
```

## Files to Create

### 1. `daily_pipeline.sh`
**Purpose**: Enhanced bash orchestrator with logging and metrics
**Location**: `/home/davsean/Documents/git/omicidx-gh-etl/daily_pipeline.sh`
**Features**:
- Replace existing `daily.sh`
- Structured logging to files
- JSON metrics tracking
- Task-level success/failure tracking
- Duration tracking per task
- Exit code handling

### 2. `omicidx_etl/transform.py`
**Purpose**: DuckDB transformation module
**Location**: `/home/davsean/Documents/git/omicidx-gh-etl/omicidx_etl/transform.py`
**Features**:
- DuckDB transformations (aggregations, joins, etc.)
- Data quality checks
- Output to parquet
- CLI interface via Click
- Uses existing `db.py` connection

### 3. `omicidx_etl/catalog.py`
**Purpose**: Metadata cataloging
**Location**: `/home/davsean/Documents/git/omicidx-gh-etl/omicidx_etl/catalog.py`
**Features**:
- Scan all parquet files
- Extract row counts, schemas, file sizes
- Create catalog.parquet with metadata
- CLI interface

### 4. `omicidx_etl/status.py`
**Purpose**: Status dashboard CLI
**Location**: `/home/davsean/Documents/git/omicidx-gh-etl/omicidx_etl/status.py`
**Features**:
- View recent pipeline runs
- Success/failure rates
- Task duration trends
- Rich terminal UI (using `rich` library)

### 5. `systemd/omicidx-daily.service` (Optional)
**Purpose**: Systemd service definition
**Location**: `/home/davsean/Documents/git/omicidx-gh-etl/systemd/omicidx-daily.service`
**Features**:
- Systemd service for scheduled runs
- Alternative to cron
- Better logging integration

### 6. `systemd/omicidx-daily.timer` (Optional)
**Purpose**: Systemd timer definition
**Location**: `/home/davsean/Documents/git/omicidx-gh-etl/systemd/omicidx-daily.timer`

## Files to Modify

### 1. `omicidx_etl/cli.py`
**Changes**:
- Add `transform` command group
- Add `catalog` command group
- Add `status` command

```python
from omicidx_etl.transform import transform
from omicidx_etl.catalog import catalog
from omicidx_etl.status import status

cli.add_command(transform)
cli.add_command(catalog)
cli.add_command(status)
```

### 2. `pyproject.toml`
**Changes**:
- Ensure `rich` is in dependencies (for status UI)
- Already has `duckdb`, `loguru`, `click`

### 3. `README.md`
**Changes**:
- Document new pipeline commands
- Document observability features
- Add examples of status command usage

### 4. `.gitignore`
**Changes**:
- Add `/metrics/` directory
- Add log directories

## Implementation Steps

### Phase 1: Foundation (Days 1-2)

#### Step 1.1: Create Transform Module
```bash
# Create the file
touch omicidx_etl/transform.py
```

**Implementation checklist**:
- [ ] Create `run_transformations()` function
- [ ] Add example DuckDB queries (SRA summary, biosample aggregations)
- [ ] Implement error handling
- [ ] Add Click CLI interface
- [ ] Write docstrings
- [ ] Test locally: `python -m omicidx_etl.transform run`

**Testing**:
```bash
# Test transform module standalone
python -m omicidx_etl.transform run --extract-dir /data/davsean/omicidx_root

# Verify output files created
ls -lh /data/davsean/omicidx_root/transformed/
```

#### Step 1.2: Create Catalog Module
```bash
touch omicidx_etl/catalog.py
```

**Implementation checklist**:
- [ ] Create `build_catalog()` function
- [ ] Use DuckDB `parquet_metadata()` function
- [ ] Export to catalog.parquet
- [ ] Add Click CLI interface
- [ ] Test locally

**Testing**:
```bash
# Test catalog module
python -m omicidx_etl.catalog build --extract-dir /data/davsean/omicidx_root

# Verify catalog created
duckdb -c "SELECT * FROM '/data/davsean/omicidx_root/catalog.parquet' LIMIT 5"
```

#### Step 1.3: Update CLI
- [ ] Add imports to `cli.py`
- [ ] Register new commands
- [ ] Test: `oidx --help` shows new commands

**Testing**:
```bash
# Verify new commands appear
oidx --help | grep -E "(transform|catalog|status)"

# Test individual commands
oidx transform --help
oidx catalog --help
```

### Phase 2: Observability (Days 3-4)

#### Step 2.1: Create Enhanced Pipeline Script
```bash
# Backup existing script
cp daily.sh daily.sh.backup

# Create new pipeline
touch daily_pipeline.sh
chmod +x daily_pipeline.sh
```

**Implementation checklist**:
- [ ] Set up logging directories
- [ ] Implement `log()` function
- [ ] Implement `run_task()` function with timing
- [ ] Create JSON metrics structure
- [ ] Add all extraction tasks
- [ ] Add transform task
- [ ] Add catalog task
- [ ] Implement error handling
- [ ] Add summary reporting

**Testing**:
```bash
# Test locally (dry run - comment out actual commands first)
./daily_pipeline.sh

# Verify logs created
ls -lh /var/log/omicidx/

# Verify metrics created
cat /data/davsean/omicidx_root/metrics/run_*.json | jq .
```

#### Step 2.2: Create Status Dashboard
```bash
touch omicidx_etl/status.py
```

**Implementation checklist**:
- [ ] Implement `get_recent_runs()` function
- [ ] Create summary statistics
- [ ] Build Rich tables for display
- [ ] Add filtering options (days, status)
- [ ] Add detailed view of latest run
- [ ] Register with CLI

**Testing**:
```bash
# After running pipeline at least once
oidx status

# Test with different options
oidx status --days 30
oidx status --help
```

### Phase 3: Integration & Testing (Day 5)

#### Step 3.1: End-to-End Test
```bash
# Run complete pipeline
./daily_pipeline.sh

# Check all components
oidx status
ls -lh /data/davsean/omicidx_root/transformed/
ls -lh /data/davsean/omicidx_root/metrics/
```

**Validation checklist**:
- [ ] All extraction tasks run
- [ ] Transformations complete
- [ ] Catalog generated
- [ ] Metrics JSON created with all tasks
- [ ] Logs written to files
- [ ] Status command shows run
- [ ] Failures are tracked correctly (test by breaking one command)

#### Step 3.2: Error Handling Test
```bash
# Intentionally break a command to test error handling
# Edit daily_pipeline.sh temporarily to use wrong path
./daily_pipeline.sh

# Verify:
# - Pipeline continues after failure
# - Error logged
# - Metrics show failed task
# - Exit code is non-zero
echo $?

# Check status shows failure
oidx status
```

#### Step 3.3: Documentation
- [ ] Update README.md with new commands
- [ ] Document pipeline structure
- [ ] Add troubleshooting section
- [ ] Document metrics location and format

### Phase 4: Deployment (Day 6)

#### Step 4.1: Set Up Directories
```bash
# Create required directories
sudo mkdir -p /var/log/omicidx
sudo chown davsean:davsean /var/log/omicidx

mkdir -p /data/davsean/omicidx_root/metrics
mkdir -p /data/davsean/omicidx_root/transformed
```

#### Step 4.2: Schedule Pipeline

**Option A: Cron (Simpler)**
```bash
# Edit crontab
crontab -e

# Add line (runs at 2 AM daily):
0 2 * * * /home/davsean/Documents/git/omicidx-gh-etl/daily_pipeline.sh >> /var/log/omicidx/cron.log 2>&1
```

**Option B: Systemd Timer (More robust)**
```bash
# Copy service files
sudo cp systemd/omicidx-daily.service /etc/systemd/system/
sudo cp systemd/omicidx-daily.timer /etc/systemd/system/

# Enable and start timer
sudo systemctl daemon-reload
sudo systemctl enable omicidx-daily.timer
sudo systemctl start omicidx-daily.timer

# Check status
sudo systemctl status omicidx-daily.timer
```

#### Step 4.3: Set Up Monitoring

**Log Rotation**:
```bash
# Create logrotate config
sudo tee /etc/logrotate.d/omicidx <<EOF
/var/log/omicidx/*.log {
    daily
    rotate 30
    compress
    delaycompress
    missingok
    notifempty
}
EOF
```

**Email Alerts** (Optional):
```bash
# Add to end of daily_pipeline.sh
if [ $EXIT_CODE -ne 0 ]; then
    echo "Pipeline failed. See logs at $LOG_FILE" | \
        mail -s "OmicIDX Pipeline Failed" your-email@example.com
fi
```

### Phase 5: Future Enhancements (Optional)

#### Enhancement 1: Metrics API
Simple Flask app to serve metrics as JSON API:
```python
# omicidx_etl/api.py
from flask import Flask, jsonify
app = Flask(__name__)

@app.route("/metrics/recent")
def recent_metrics():
    # Return last 10 runs
    pass

if __name__ == "__main__":
    app.run(port=8080)
```

#### Enhancement 2: Slack Notifications
```python
# Add to daily_pipeline.sh or Python modules
def send_slack_notification(webhook_url, message):
    import httpx
    httpx.post(webhook_url, json={"text": message})
```

#### Enhancement 3: Data Quality Checks
```python
# omicidx_etl/quality.py
def check_data_quality(extract_dir):
    """Run DuckDB data quality checks."""
    with duckdb_connection() as con:
        # Check for duplicates
        # Check for null required fields
        # Check date ranges
        # Check record counts vs previous run
    pass
```

## Timeline

| Phase | Duration | Deliverables |
|-------|----------|--------------|
| Phase 1: Foundation | 2 days | Transform module, Catalog module, CLI integration |
| Phase 2: Observability | 2 days | Enhanced pipeline script, Status dashboard |
| Phase 3: Integration | 1 day | Testing, documentation |
| Phase 4: Deployment | 1 day | Scheduling, monitoring setup |
| **Total** | **6 days** | **Production-ready observability system** |

## Success Criteria

- [ ] Pipeline runs daily without manual intervention
- [ ] All tasks tracked with success/failure status
- [ ] Metrics collected and viewable via `oidx status`
- [ ] Logs retained for 30 days
- [ ] Transformations produce expected output files
- [ ] Data catalog updated after each run
- [ ] Failed tasks don't block entire pipeline
- [ ] Easy to debug issues from logs and metrics
- [ ] Documentation complete for handoff

## Rollback Plan

If issues arise:
1. Revert to `daily.sh.backup`
2. Continue using existing extraction commands
3. Remove new CLI commands from `cli.py`
4. Delete new modules

## File Structure After Implementation

```
omicidx-gh-etl/
├── daily_pipeline.sh           # New enhanced orchestrator
├── daily.sh.backup             # Backup of original
├── omicidx_etl/
│   ├── transform.py            # New: transformations
│   ├── catalog.py              # New: metadata catalog
│   ├── status.py               # New: status dashboard
│   ├── cli.py                  # Modified: add new commands
│   └── [existing modules...]
├── systemd/                     # New: optional systemd files
│   ├── omicidx-daily.service
│   └── omicidx-daily.timer
└── OBSERVABILITY_PLAN.md       # This document

/data/davsean/omicidx_root/
├── sra/                         # Existing extraction output
├── biosample/
├── ebi_biosample/
├── geo/
├── transformed/                 # New: transformation output
│   ├── sra_summary.parquet
│   └── [other transformed data]
├── metrics/                     # New: pipeline metrics
│   ├── run_20251018_020000.json
│   └── [historical runs...]
└── catalog.parquet             # New: metadata catalog

/var/log/omicidx/               # New: logs directory
├── run_20251018_020000.log
└── [historical logs...]
```

## Dependencies

All required dependencies already in `pyproject.toml`:
- ✅ `duckdb` - transformations
- ✅ `click` - CLI
- ✅ `loguru` - logging
- ✅ `rich` - status UI (verify it's installed)
- ✅ `pathlib` - file handling
- ✅ `json` - metrics

**Check:**
```bash
python -c "import rich" || pip install rich
```

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Pipeline breaks existing jobs | Low | High | Keep old daily.sh as backup, test thoroughly |
| Logs fill disk | Medium | Medium | Log rotation configured, 30-day retention |
| Metrics grow too large | Low | Low | One JSON per run, small files |
| DuckDB transformations timeout | Medium | Medium | Set reasonable timeouts, alert on failure |
| Cron failures go unnoticed | Medium | High | Add email alerts, check status regularly |

## Questions to Answer Before Starting

1. **Where should logs be stored?**
   - Proposal: `/var/log/omicidx/`
   - Alternative: `/data/davsean/omicidx_root/logs/`

2. **What transformations are needed?**
   - List specific DuckDB queries/aggregations
   - What summary tables to create?
   - What joins between datasets?

3. **Scheduling preference?**
   - Cron (simpler)
   - Systemd timer (more robust)

4. **Retention policy?**
   - Logs: 30 days (proposed)
   - Metrics: Keep all? Archive monthly?

5. **Alert recipients?**
   - Email addresses for failure notifications
   - Slack channel? (if needed)

## Next Steps

1. **Review this plan** - adjust timeline/scope as needed
2. **Answer questions above** - clarify requirements
3. **Start Phase 1** - implement transform & catalog modules
4. **Iterate through phases** - test at each step
5. **Deploy to production** - schedule and monitor

---

**Document Version**: 1.0
**Last Updated**: 2025-10-18
**Author**: Claude Code assisted implementation
