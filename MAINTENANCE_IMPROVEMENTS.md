# omicidx_etl Package - Maintenance Improvements

This document tracks improvements to enhance maintainability and homogeneity across the omicidx_etl package.

## ✅ Completed (High Priority)

- [x] Add missing `__init__.py` to ebi_biosample/
- [x] Remove hard-coded path from ebi_biosample/extract.py
- [x] Remove hard-coded path from geo/extract.py
- [x] Remove logging.basicConfig() from geo/extract.py (standardize on loguru)
- [x] Refactor etl/scimago.py to remove Prefect and use Click

## ✅ Completed (Medium Priority)

- [x] **Fix unused CLI argument in ebi_biosample** - The extract command now properly accepts and uses the `--output-dir` option (converted from unused argument to functional optional flag with default)
- [x] **Standardize async library usage** - All modules now use anyio consistently (ebi_biosample converted from asyncio.run to anyio.run)

## Medium Priority Improvements

### 5. Add Missing Documentation

Three modules lack README.md files that would help with onboarding and maintenance:

- [ ] **ebi_biosample/README.md**
  - Document purpose: EBI Biosamples data extraction
  - Explain date-based filtering and monthly processing
  - Document output format (NDJSON.gz)
  - Include usage examples
  - Reference: See biosample/README.md and sra/README.md for structure

- [ ] **geo/README.md**
  - Document purpose: GEO (Gene Expression Omnibus) metadata extraction
  - Explain GSE/GSM/GPL entity types
  - Document async processing with 30 workers
  - Document output format (NDJSON.gz per entity type)
  - Include usage examples

- [ ] **etl/README.md**
  - Document purpose: Citation and publication data from multiple sources
  - List all data sources (iCite, PubMed, Europe PMC, Scimago)
  - Document each module's purpose and output format
  - Include usage examples for each command

### 6. Consolidate Module Structure

**Issue:** The sra module is the only one with a separate `cli.py` file

**Files:**
- `omicidx_etl/sra/cli.py` - separate CLI file
- `omicidx_etl/sra/extract.py` - extraction logic

**Options:**
- **Option A:** Keep current pattern (most modules mix CLI + logic)
- **Option B:** Extract CLI to separate files for all modules (like sra)

**Recommendation:** Option A - document this as the standard. Consider refactoring sra to match if it's not complex.

## Low Priority Improvements

### 7. Standardize Date Range Functions

**Issue:** Both ebi_biosample and geo have nearly identical date range functions

**Files:**
- `omicidx_etl/ebi_biosample/extract.py:150-174` - `get_date_ranges()`
- `omicidx_etl/geo/extract.py:329-354` - `get_monthly_ranges()`

**Recommendation:** Extract to shared utility module

```python
# Create: omicidx_etl/utils.py
def get_monthly_date_ranges(start_date_str: str, end_date_str: str) -> Iterable[tuple[date, date]]:
    """Generate monthly date ranges between start and end dates.

    Args:
        start_date_str: Start date in 'YYYY-MM-DD' format
        end_date_str: End date in 'YYYY-MM-DD' format

    Yields:
        Tuples of (month_start_date, month_end_date)
    """
    ...
```

### 8. Standardize Retry Configuration

**Issue:** Different modules use slightly different tenacity retry configurations

**Files:**
- `omicidx_etl/ebi_biosample/extract.py:70-76`
- `omicidx_etl/geo/extract.py:36-51`
- Other modules

**Recommendation:** Create shared retry decorators in utils module

```python
# In omicidx_etl/utils.py
from tenacity import retry, stop_after_attempt, wait_random_exponential
from loguru import logger

# Standard HTTP retry decorator
http_retry = retry(
    stop=stop_after_attempt(10),
    wait=wait_random_exponential(multiplier=1, max=40),
    before_sleep=lambda retry_state: logger.warning(
        f"Request failed, retrying in {retry_state.upcoming_sleep}s "
        f"(attempt {retry_state.attempt_number}/10)"
    ),
)
```

### 9. Unused Import Cleanup

Minor code quality improvements:

- [ ] `omicidx_etl/ebi_biosample/extract.py:171` - review variable usage
- [ ] `omicidx_etl/geo/extract.py:277` - move `time` import to top of file
- [ ] Run linter to find other unused imports

### 10. Add Type Hints Consistently

**Issue:** Some modules have better type hints than others

**Recommendation:** Add comprehensive type hints to all public functions

**Benefits:**
- Better IDE support
- Catch errors earlier
- Self-documenting code

**Files needing improvement:**
- Most modules could benefit from more complete type annotations
- Use `from typing import` for Optional, List, Dict, etc.

## Testing Recommendations (Future)

Consider adding:
- Unit tests for utility functions (date ranges, retry logic)
- Integration tests for data extraction (using mock data)
- CI/CD pipeline to run tests on changes

## Documentation Recommendations (Future)

Consider adding:
- Top-level README.md with architecture overview
- Contributing guidelines (CONTRIBUTING.md)
- Changelog (CHANGELOG.md) to track changes
- API documentation (Sphinx or MkDocs)

## Implementation Notes

### How to Resume Work

1. Pick an item from the list above
2. Create a git branch if desired: `git checkout -b feature/add-docs`
3. Make the changes
4. Commit with descriptive message
5. Test the changes
6. Merge when ready

### Commit Message Format

Follow the established pattern:

```
Short description (50 chars or less)

Longer explanation of the change, why it was needed, and any
relevant context. Explain both what and why.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>
```

### Testing Changes

Before committing, test that the CLI commands still work:

```bash
# Test individual modules
python -m omicidx_etl.ebi_biosample.extract
python -m omicidx_etl.geo.extract
python -m omicidx_etl.etl.scimago

# Or through main CLI
python -m omicidx_etl.cli --help
```

## Questions or Issues?

If you have questions about any of these improvements:
- Reference this document and the git history
- Check the completed changes (commits ccbf174 through 0f92bd1)
- Look at existing modules for patterns (biosample, sra are good examples)

---

**Last Updated:** 2025-10-17
**Status:** High-priority items complete, medium/low-priority items pending
