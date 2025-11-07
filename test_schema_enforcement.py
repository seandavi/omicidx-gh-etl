"""
Test script to verify schema enforcement in SRA extraction.

This script tests:
1. Schema definitions are correctly loaded
2. Record normalization handles null vs empty list issues
3. All record types have proper schemas
"""

from omicidx_etl.sra.extract import (
    PYARROW_SCHEMAS,
    normalize_record,
    infer_record_type_from_url
)


def test_schemas_loaded():
    """Test that all schemas are properly loaded."""
    print("Testing schema loading...")
    assert PYARROW_SCHEMAS, "Schemas not loaded"

    expected_types = ["run", "study", "sample", "experiment"]
    for record_type in expected_types:
        assert record_type in PYARROW_SCHEMAS, f"Schema for {record_type} not found"
        schema = PYARROW_SCHEMAS[record_type]
        print(f"  ✓ {record_type}: {len(schema)} fields")

    print("✓ All schemas loaded successfully\n")


def test_record_normalization():
    """Test that record normalization handles null vs empty list issues."""
    print("Testing record normalization...")

    # Test study record with missing fields
    study_record = {
        "accession": "SRP123456",
        "study_accession": "SRP123456",
        "title": "Test Study",
        "identifiers": None,  # Should become []
        "attributes": None,   # Should become []
        "xrefs": None,        # Should become []
        "pubmed_ids": None,   # Should become []
    }

    normalized = normalize_record(study_record, "study")

    assert normalized["identifiers"] == [], f"Expected [], got {normalized['identifiers']}"
    assert normalized["attributes"] == [], f"Expected [], got {normalized['attributes']}"
    assert normalized["xrefs"] == [], f"Expected [], got {normalized['xrefs']}"
    assert normalized["pubmed_ids"] == [], f"Expected [], got {normalized['pubmed_ids']}"

    print("  ✓ Null lists converted to empty lists")

    # Test run record with missing fields
    run_record = {
        "accession": "SRR123456",
        "experiment_accession": "SRX123456",
        "identifiers": [{"id": "test", "namespace": "test", "uuid": None}],
        "files": None,        # Should become []
        "reads": None,        # Should become []
        "qualities": None,    # Should become []
    }

    normalized = normalize_record(run_record, "run")

    assert normalized["files"] == [], f"Expected [], got {normalized['files']}"
    assert normalized["reads"] == [], f"Expected [], got {normalized['reads']}"
    assert normalized["qualities"] == [], f"Expected [], got {normalized['qualities']}"
    assert len(normalized["identifiers"]) == 1, "Identifiers should be preserved"

    print("  ✓ Run record normalized correctly")
    print("✓ Record normalization working\n")


def test_url_inference():
    """Test URL-based record type inference."""
    print("Testing URL inference...")

    test_cases = [
        ("https://ftp.ncbi.nlm.nih.gov/sra/reports/Mirroring/NCBI_SRA_Mirroring_20250101_Full/meta_run_set.xml.gz", "run"),
        ("https://ftp.ncbi.nlm.nih.gov/sra/reports/Mirroring/NCBI_SRA_Mirroring_20250101_Full/meta_study_set.xml.gz", "study"),
        ("https://ftp.ncbi.nlm.nih.gov/sra/reports/Mirroring/NCBI_SRA_Mirroring_20250101_Full/meta_sample_set.xml.gz", "sample"),
        ("https://ftp.ncbi.nlm.nih.gov/sra/reports/Mirroring/NCBI_SRA_Mirroring_20250101_Full/meta_experiment_set.xml.gz", "experiment"),
    ]

    for url, expected_type in test_cases:
        inferred = infer_record_type_from_url(url)
        assert inferred == expected_type, f"Expected {expected_type}, got {inferred} for {url}"
        print(f"  ✓ {expected_type}: correctly inferred")

    print("✓ URL inference working\n")


def print_schema_summary():
    """Print a summary of all schemas."""
    print("Schema Summary:")
    print("=" * 60)

    for record_type, schema in PYARROW_SCHEMAS.items():
        print(f"\n{record_type.upper()} Schema ({len(schema)} fields):")
        print("-" * 60)

        # Count field types
        list_fields = []
        struct_fields = []
        scalar_fields = []

        for field in schema:
            field_type_str = str(field.type)
            if field_type_str.startswith("list"):
                list_fields.append(field.name)
            elif field_type_str.startswith("struct"):
                struct_fields.append(field.name)
            else:
                scalar_fields.append(field.name)

        print(f"  Scalar fields ({len(scalar_fields)}): {', '.join(scalar_fields[:5])}" +
              (f", ... (+{len(scalar_fields)-5} more)" if len(scalar_fields) > 5 else ""))
        print(f"  List fields ({len(list_fields)}): {', '.join(list_fields)}")
        print(f"  Struct fields ({len(struct_fields)}): {', '.join(struct_fields)}")


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("SRA Schema Enforcement Test")
    print("=" * 60 + "\n")

    try:
        test_schemas_loaded()
        test_record_normalization()
        test_url_inference()
        print_schema_summary()

        print("\n" + "=" * 60)
        print("✓ All tests passed!")
        print("=" * 60 + "\n")

    except AssertionError as e:
        print(f"\n✗ Test failed: {e}\n")
        raise
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}\n")
        raise
