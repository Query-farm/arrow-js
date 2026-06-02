#!/usr/bin/env python3
"""
Record Batch Writer with Custom Metadata

This script demonstrates how to use PyArrow to:
1. Create a RecordBatch with various data types
2. Attach custom metadata at the schema level and field level
3. Serialize the RecordBatch to an Arrow IPC file
4. Read it back and verify all metadata is preserved

The custom metadata includes edge cases designed to thoroughly test
serialization/deserialization:
- Unicode characters (emoji, CJK, RTL text, diacritics)
- Binary-like data encoded as base64
- JSON structures
- Empty strings and whitespace-only values
- Very long strings
- Special characters and escape sequences

Usage:
    python record-batch-writer-with-metadata.py [output_file]

If no output file is specified, defaults to 'test_record_batch.arrow'
"""

import sys
import json
import base64
from pathlib import Path
from datetime import datetime, timezone

import pyarrow as pa


# =============================================================================
# CUSTOM METADATA DEFINITIONS
# =============================================================================

def create_schema_metadata() -> dict[bytes, bytes]:
    """
    Create schema-level metadata with various edge cases for testing.

    Arrow metadata requires both keys and values to be bytes.
    We include various types of content to stress-test serialization.

    Returns:
        Dictionary with bytes keys and bytes values for schema metadata.
    """
    metadata = {}

    # ---------------------------------------------------------------------
    # Basic metadata - simple key-value pairs
    # ---------------------------------------------------------------------
    metadata[b"created_by"] = b"record-batch-writer-with-metadata.py"
    metadata[b"version"] = b"1.0.0"
    metadata[b"created_at"] = datetime.now(timezone.utc).isoformat().encode("utf-8")

    # ---------------------------------------------------------------------
    # Unicode stress tests - various scripts and special characters
    # ---------------------------------------------------------------------

    # Emoji (tests 4-byte UTF-8 sequences)
    metadata[b"unicode_emoji"] = "🚀🎉💾📊🔥✨🌍🎯".encode("utf-8")

    # CJK characters (Chinese, Japanese, Korean)
    metadata[b"unicode_cjk"] = "中文日本語한국어".encode("utf-8")

    # Right-to-left text (Arabic, Hebrew)
    metadata[b"unicode_rtl"] = "مرحبا שלום".encode("utf-8")

    # Diacritics and combining characters
    metadata[b"unicode_diacritics"] = "café naïve résumé Ñoño".encode("utf-8")

    # Mathematical and technical symbols
    metadata[b"unicode_math"] = "∑∏∫∂∇∆≈≠≤≥±×÷√∞".encode("utf-8")

    # Mixed script text (realistic multilingual content)
    metadata[b"unicode_mixed"] = "Hello 世界! Привет мир! 🌍".encode("utf-8")

    # ---------------------------------------------------------------------
    # JSON structure - nested data encoded as JSON string
    # ---------------------------------------------------------------------
    json_metadata = {
        "source": {
            "system": "test_generator",
            "version": "2.0",
            "environment": "development"
        },
        "processing": {
            "steps": ["validate", "transform", "enrich"],
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "options": {
                "strict_mode": True,
                "null_handling": "skip",
                "encoding": "utf-8"
            }
        },
        "statistics": {
            "row_count": 5,
            "null_count": 2,
            "data_quality_score": 0.95
        },
        "tags": ["test", "sample", "metadata-test", "arrow-ipc"],
        "nested_array": [[1, 2], [3, 4, 5], []],
        "unicode_in_json": {
            "greeting": "Hello 你好 مرحبا",
            "emoji": "🎉"
        }
    }
    metadata[b"json_config"] = json.dumps(json_metadata, ensure_ascii=False).encode("utf-8")

    # ---------------------------------------------------------------------
    # Binary data - base64 encoded to safely store arbitrary bytes
    # ---------------------------------------------------------------------

    # Simulated binary blob (like a small image header or signature)
    binary_data = bytes([
        0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A,  # PNG signature
        0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52,  # IHDR chunk start
        0xFF, 0xFE, 0xFD, 0x00, 0x01, 0x02, 0x03, 0x04   # Some bytes
    ])
    metadata[b"binary_blob_b64"] = base64.b64encode(binary_data)

    # The raw binary data (Arrow metadata supports arbitrary bytes)
    metadata[b"binary_raw"] = binary_data

    # ---------------------------------------------------------------------
    # Edge cases - empty, whitespace, special characters
    # ---------------------------------------------------------------------

    # Empty value (valid but empty)
    metadata[b"empty_value"] = b""

    # Whitespace only
    metadata[b"whitespace_only"] = b"   \t\n\r   "

    # Special characters and escape sequences
    metadata[b"special_chars"] = b"line1\nline2\ttabbed\r\nwindows-newline"

    # Quotes and backslashes
    metadata[b"quotes_and_escapes"] = b'He said "Hello" and used a \\backslash\\'

    # Null bytes embedded in string (Arrow should handle this)
    metadata[b"with_null_bytes"] = b"before\x00middle\x00after"

    # ---------------------------------------------------------------------
    # Long string - tests buffer handling
    # ---------------------------------------------------------------------

    # 10KB of repeated pattern
    long_pattern = "Apache Arrow is great! " * 500  # ~11.5KB
    metadata[b"long_string"] = long_pattern.encode("utf-8")

    # ---------------------------------------------------------------------
    # Numeric strings - ensure they're not converted to numbers
    # ---------------------------------------------------------------------
    metadata[b"numeric_string_int"] = b"1234567890"
    metadata[b"numeric_string_float"] = b"3.14159265358979"
    metadata[b"numeric_string_scientific"] = b"1.23e-45"

    # ---------------------------------------------------------------------
    # Path-like and URL-like strings
    # ---------------------------------------------------------------------
    metadata[b"file_path"] = b"/path/to/data/file.arrow"
    metadata[b"url"] = b"https://arrow.apache.org/docs/?query=metadata&lang=en#section"

    return metadata


def create_field_metadata(field_name: str) -> dict[bytes, bytes]:
    """
    Create field-level metadata specific to each column.

    Field metadata can store column-specific information like:
    - Data lineage (where the column came from)
    - Validation rules
    - Display formatting hints
    - Business logic descriptions

    Args:
        field_name: Name of the field to create metadata for.

    Returns:
        Dictionary with bytes keys and bytes values for field metadata.
    """
    # Base metadata common to all fields
    metadata = {
        b"field_name": field_name.encode("utf-8"),
        b"created_by": b"record-batch-writer-with-metadata.py",
    }

    # Field-specific metadata
    field_configs = {
        "id": {
            b"description": b"Unique identifier for each record",
            b"validation": b"positive_integer",
            b"primary_key": b"true",
            b"indexed": b"true",
        },
        "name": {
            b"description": b"Person's full name - supports Unicode",
            b"validation": b"non_empty_string",
            b"max_length": b"255",
            b"searchable": b"true",
            b"unicode_test": "Contains: émojis 🎉, CJK 中文, RTL عربي".encode("utf-8"),
        },
        "score": {
            b"description": b"Performance score as floating point",
            b"validation": b"range:0.0-100.0",
            b"precision": b"2",
            b"unit": b"percentage",
            b"aggregations": b"sum,avg,min,max",
        },
        "active": {
            b"description": b"Whether the record is currently active",
            b"validation": b"boolean",
            b"default": b"true",
            b"filter_hint": b"commonly_filtered",
        },
        "tags": {
            b"description": b"List of string tags for categorization",
            b"validation": b"string_list",
            b"separator": b",",
            b"max_items": b"10",
            b"example": b'["important", "reviewed", "pending"]',
        },
        "created_date": {
            b"description": b"Record creation timestamp",
            b"validation": b"iso8601_date",
            b"timezone": b"UTC",
            b"format_hint": b"YYYY-MM-DD",
        },
        "metadata_json": {
            b"description": b"Arbitrary JSON metadata stored as string",
            b"validation": b"valid_json",
            b"content_type": b"application/json",
            b"schema_version": b"1.0",
        },
    }

    # Add field-specific config if available
    if field_name in field_configs:
        metadata.update(field_configs[field_name])

    return metadata


# =============================================================================
# RECORD BATCH CREATION
# =============================================================================

def create_test_record_batch() -> pa.RecordBatch:
    """
    Create a RecordBatch with diverse data types and comprehensive metadata.

    The data includes:
    - Integer column (id)
    - String column with Unicode (name)
    - Float column with nulls (score)
    - Boolean column (active)
    - List of strings column (tags)
    - Date column (created_date)
    - String column with JSON (metadata_json)

    Returns:
        PyArrow RecordBatch with schema and field metadata attached.
    """
    # -------------------------------------------------------------------------
    # Define the data for each column
    # -------------------------------------------------------------------------

    # Integer IDs
    ids = [1, 2, 3, 4, 5]

    # Names with various Unicode content to test string handling
    names = [
        "Alice Johnson",           # Plain ASCII
        "José García",             # Spanish diacritics
        "田中太郎",                 # Japanese Kanji
        "محمد أحمد",               # Arabic (RTL)
        "Élise Müller 🎉",         # French/German + emoji
    ]

    # Scores with some null values to test null handling
    scores = [95.5, None, 87.3, 92.1, None]

    # Boolean activity flags
    active = [True, False, True, True, False]

    # List of tags (variable length lists)
    tags = [
        ["admin", "verified"],
        ["guest"],
        ["user", "premium", "早期アクセス"],  # Japanese tag
        ["user"],
        None,  # Null list
    ]

    # Dates
    from datetime import date
    created_dates = [
        date(2024, 1, 15),
        date(2024, 2, 20),
        date(2024, 3, 10),
        date(2024, 4, 5),
        date(2024, 5, 1),
    ]

    # JSON metadata as strings (nested structures)
    metadata_json = [
        json.dumps({"level": 5, "permissions": ["read", "write", "admin"]}),
        json.dumps({"level": 1, "permissions": ["read"]}),
        json.dumps({"level": 3, "preferences": {"theme": "dark", "lang": "ja"}}),
        json.dumps({"level": 2, "permissions": ["read", "write"]}),
        None,  # Null JSON
    ]

    # -------------------------------------------------------------------------
    # Create PyArrow arrays for each column
    # -------------------------------------------------------------------------

    id_array = pa.array(ids, type=pa.int64())
    name_array = pa.array(names, type=pa.utf8())
    score_array = pa.array(scores, type=pa.float64())
    active_array = pa.array(active, type=pa.bool_())
    tags_array = pa.array(tags, type=pa.list_(pa.utf8()))
    date_array = pa.array(created_dates, type=pa.date32())
    json_array = pa.array(metadata_json, type=pa.utf8())

    # -------------------------------------------------------------------------
    # Create fields with metadata attached to each
    # -------------------------------------------------------------------------

    fields = [
        pa.field("id", pa.int64(),
                 nullable=False,
                 metadata=create_field_metadata("id")),
        pa.field("name", pa.utf8(),
                 nullable=False,
                 metadata=create_field_metadata("name")),
        pa.field("score", pa.float64(),
                 nullable=True,
                 metadata=create_field_metadata("score")),
        pa.field("active", pa.bool_(),
                 nullable=False,
                 metadata=create_field_metadata("active")),
        pa.field("tags", pa.list_(pa.utf8()),
                 nullable=True,
                 metadata=create_field_metadata("tags")),
        pa.field("created_date", pa.date32(),
                 nullable=False,
                 metadata=create_field_metadata("created_date")),
        pa.field("metadata_json", pa.utf8(),
                 nullable=True,
                 metadata=create_field_metadata("metadata_json")),
    ]

    # -------------------------------------------------------------------------
    # Create schema with schema-level metadata
    # -------------------------------------------------------------------------

    schema = pa.schema(fields, metadata=create_schema_metadata())

    # -------------------------------------------------------------------------
    # Create and return the RecordBatch
    # -------------------------------------------------------------------------

    return pa.RecordBatch.from_arrays(
        [id_array, name_array, score_array, active_array,
         tags_array, date_array, json_array],
        schema=schema
    )


# =============================================================================
# FILE I/O OPERATIONS
# =============================================================================

def write_record_batch(record_batch: pa.RecordBatch, filepath: Path) -> None:
    """
    Write a RecordBatch to an Arrow IPC file.

    Uses the IPC file format (also called Feather v2) which:
    - Supports random access to record batches
    - Preserves all metadata
    - Is the recommended format for file storage

    Args:
        record_batch: The RecordBatch to write.
        filepath: Path where the file will be written.
    """
    print(f"Writing RecordBatch to: {filepath}")
    print(f"  - Rows: {record_batch.num_rows}")
    print(f"  - Columns: {record_batch.num_columns}")
    print(f"  - Schema metadata keys: {len(record_batch.schema.metadata or {})}")

    # Create an IPC file writer
    # We use the RecordBatchFileWriter for the IPC file format
    with pa.OSFile(str(filepath), 'wb') as sink:
        with pa.ipc.new_file(sink, record_batch.schema) as writer:
            writer.write_batch(record_batch)

    file_size = filepath.stat().st_size
    print(f"  - File size: {file_size:,} bytes")


def read_record_batch(filepath: Path) -> pa.RecordBatch:
    """
    Read a RecordBatch from an Arrow IPC file.

    Args:
        filepath: Path to the Arrow IPC file.

    Returns:
        The RecordBatch read from the file.
    """
    print(f"\nReading RecordBatch from: {filepath}")

    with pa.OSFile(str(filepath), 'rb') as source:
        reader = pa.ipc.open_file(source)
        # IPC files can contain multiple batches; we read the first one
        record_batch = reader.get_batch(0)

    print(f"  - Rows: {record_batch.num_rows}")
    print(f"  - Columns: {record_batch.num_columns}")

    return record_batch


# =============================================================================
# VERIFICATION AND DISPLAY
# =============================================================================

def verify_metadata(original: pa.RecordBatch, loaded: pa.RecordBatch) -> bool:
    """
    Verify that all metadata was preserved during serialization.

    Compares:
    - Schema-level metadata
    - Field-level metadata for each column
    - Data types and nullability

    Args:
        original: The RecordBatch before serialization.
        loaded: The RecordBatch after deserialization.

    Returns:
        True if all metadata matches, False otherwise.
    """
    print("\n" + "=" * 70)
    print("VERIFICATION RESULTS")
    print("=" * 70)

    all_passed = True

    # -------------------------------------------------------------------------
    # Verify schema-level metadata
    # -------------------------------------------------------------------------
    print("\n1. Schema-level metadata:")

    orig_meta = original.schema.metadata or {}
    load_meta = loaded.schema.metadata or {}

    if orig_meta.keys() != load_meta.keys():
        print(f"   ❌ FAILED: Metadata key mismatch")
        print(f"      Original keys: {len(orig_meta)}")
        print(f"      Loaded keys: {len(load_meta)}")
        all_passed = False
    else:
        mismatches = []
        for key in orig_meta:
            if orig_meta[key] != load_meta[key]:
                mismatches.append(key)

        if mismatches:
            print(f"   ❌ FAILED: {len(mismatches)} value mismatches")
            for key in mismatches[:5]:  # Show first 5
                print(f"      Key: {key}")
            all_passed = False
        else:
            print(f"   ✓ PASSED: All {len(orig_meta)} metadata entries match")

    # -------------------------------------------------------------------------
    # Verify field-level metadata
    # -------------------------------------------------------------------------
    print("\n2. Field-level metadata:")

    for i, (orig_field, load_field) in enumerate(zip(original.schema, loaded.schema)):
        orig_field_meta = orig_field.metadata or {}
        load_field_meta = load_field.metadata or {}

        if orig_field_meta != load_field_meta:
            print(f"   ❌ FAILED: Field '{orig_field.name}' metadata mismatch")
            all_passed = False
        else:
            meta_count = len(orig_field_meta)
            print(f"   ✓ Field '{orig_field.name}': {meta_count} metadata entries match")

    # -------------------------------------------------------------------------
    # Verify data integrity
    # -------------------------------------------------------------------------
    print("\n3. Data integrity:")

    if original.num_rows != loaded.num_rows:
        print(f"   ❌ FAILED: Row count mismatch ({original.num_rows} vs {loaded.num_rows})")
        all_passed = False
    else:
        print(f"   ✓ Row count: {original.num_rows}")

    if original.num_columns != loaded.num_columns:
        print(f"   ❌ FAILED: Column count mismatch")
        all_passed = False
    else:
        print(f"   ✓ Column count: {original.num_columns}")

    # Compare actual data
    if original.equals(loaded):
        print("   ✓ All data values match")
    else:
        print("   ❌ FAILED: Data values do not match")
        all_passed = False

    # -------------------------------------------------------------------------
    # Summary
    # -------------------------------------------------------------------------
    print("\n" + "-" * 70)
    if all_passed:
        print("✓ ALL VERIFICATION CHECKS PASSED")
    else:
        print("❌ SOME VERIFICATION CHECKS FAILED")
    print("-" * 70)

    return all_passed


def display_metadata_summary(record_batch: pa.RecordBatch) -> None:
    """
    Display a human-readable summary of all metadata in the RecordBatch.

    Args:
        record_batch: The RecordBatch to display metadata from.
    """
    print("\n" + "=" * 70)
    print("METADATA SUMMARY")
    print("=" * 70)

    schema = record_batch.schema

    # -------------------------------------------------------------------------
    # Schema-level metadata
    # -------------------------------------------------------------------------
    print("\nSchema Metadata:")
    print("-" * 40)

    if schema.metadata:
        for key, value in sorted(schema.metadata.items()):
            # Decode bytes for display
            key_str = key.decode('utf-8', errors='replace')

            # Truncate long values for display
            try:
                value_str = value.decode('utf-8', errors='replace')
            except Exception:
                value_str = f"<binary: {len(value)} bytes>"

            if len(value_str) > 60:
                value_str = value_str[:57] + "..."

            print(f"  {key_str}: {value_str}")
    else:
        print("  (no schema metadata)")

    # -------------------------------------------------------------------------
    # Field-level metadata
    # -------------------------------------------------------------------------
    print("\nField Metadata:")
    print("-" * 40)

    for field in schema:
        print(f"\n  [{field.name}] ({field.type}, nullable={field.nullable})")
        if field.metadata:
            for key, value in sorted(field.metadata.items()):
                key_str = key.decode('utf-8', errors='replace')
                value_str = value.decode('utf-8', errors='replace')
                if len(value_str) > 50:
                    value_str = value_str[:47] + "..."
                print(f"    {key_str}: {value_str}")
        else:
            print("    (no field metadata)")


def display_data_sample(record_batch: pa.RecordBatch) -> None:
    """
    Display a sample of the data in the RecordBatch.

    Args:
        record_batch: The RecordBatch to display.
    """
    print("\n" + "=" * 70)
    print("DATA SAMPLE")
    print("=" * 70)

    # Convert to pandas for nice display (if available)
    try:
        import pandas as pd
        df = record_batch.to_pandas()
        print(df.to_string())
    except ImportError:
        # Fallback: manual display
        print("\nColumns:", [f.name for f in record_batch.schema])
        for i in range(record_batch.num_rows):
            row = [str(record_batch.column(j)[i].as_py()) for j in range(record_batch.num_columns)]
            print(f"Row {i}: {row}")


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def main():
    """
    Main function that orchestrates the record batch creation, serialization,
    deserialization, and verification.
    """
    print("=" * 70)
    print("Apache Arrow RecordBatch Writer with Custom Metadata")
    print("=" * 70)
    print(f"PyArrow version: {pa.__version__}")

    # Determine output file path
    if len(sys.argv) > 1:
        output_file = Path(sys.argv[1])
    else:
        output_file = Path(__file__).parent / "test_record_batch.arrow"

    print(f"Output file: {output_file}")

    # -------------------------------------------------------------------------
    # Step 1: Create the RecordBatch with metadata
    # -------------------------------------------------------------------------
    print("\n" + "-" * 70)
    print("STEP 1: Creating RecordBatch with metadata")
    print("-" * 70)

    original_batch = create_test_record_batch()
    print(f"Created RecordBatch with {original_batch.num_rows} rows")

    # -------------------------------------------------------------------------
    # Step 2: Write to file
    # -------------------------------------------------------------------------
    print("\n" + "-" * 70)
    print("STEP 2: Writing to Arrow IPC file")
    print("-" * 70)

    write_record_batch(original_batch, output_file)

    # -------------------------------------------------------------------------
    # Step 3: Read back from file
    # -------------------------------------------------------------------------
    print("\n" + "-" * 70)
    print("STEP 3: Reading from Arrow IPC file")
    print("-" * 70)

    loaded_batch = read_record_batch(output_file)

    # -------------------------------------------------------------------------
    # Step 4: Verify metadata preservation
    # -------------------------------------------------------------------------
    print("\n" + "-" * 70)
    print("STEP 4: Verifying metadata preservation")
    print("-" * 70)

    verification_passed = verify_metadata(original_batch, loaded_batch)

    # -------------------------------------------------------------------------
    # Step 5: Display summary
    # -------------------------------------------------------------------------
    display_metadata_summary(loaded_batch)
    display_data_sample(loaded_batch)

    # -------------------------------------------------------------------------
    # Final result
    # -------------------------------------------------------------------------
    print("\n" + "=" * 70)
    if verification_passed:
        print("SUCCESS: RecordBatch with metadata serialized and deserialized correctly!")
    else:
        print("FAILURE: Some checks did not pass. Review the output above.")
    print("=" * 70)

    return 0 if verification_passed else 1


if __name__ == "__main__":
    sys.exit(main())
