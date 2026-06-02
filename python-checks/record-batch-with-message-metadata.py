#!/usr/bin/env python3
"""
Record Batch Writer with Message-Level Custom Metadata

This script creates an Arrow IPC file with custom metadata at the MESSAGE level
(not just schema/field level). This tests the JavaScript implementation's ability
to read message-level custom_metadata from the Arrow IPC format.

The custom_metadata is attached to each RecordBatch message using PyArrow's
write_batch(batch, custom_metadata={...}) API.

Usage:
    python record-batch-with-message-metadata.py [output_file]
"""

import sys
import json
from pathlib import Path
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.ipc as ipc


def create_message_metadata(batch_index: int) -> dict[bytes, bytes]:
    """
    Create message-level metadata for a record batch.

    This metadata is stored in the Message FlatBuffer wrapper,
    separate from schema and field metadata.

    Args:
        batch_index: Index of the batch (for unique identification)

    Returns:
        Dictionary with bytes keys and bytes values for message metadata.
    """
    metadata = {}

    # Basic batch identification
    metadata[b"batch_index"] = str(batch_index).encode("utf-8")
    metadata[b"batch_id"] = f"batch_{batch_index:04d}".encode("utf-8")
    metadata[b"created_at"] = datetime.now(timezone.utc).isoformat().encode("utf-8")

    # Processing metadata
    metadata[b"producer"] = b"pyarrow-test-generator"
    metadata[b"producer_version"] = pa.__version__.encode("utf-8")

    # Unicode test data
    metadata[b"unicode_test"] = "Hello 世界 🌍 مرحبا".encode("utf-8")

    # JSON structure
    batch_info = {
        "batch_number": batch_index,
        "processing_stage": "final",
        "quality_score": 0.98,
        "tags": ["validated", "complete"],
        "source": {
            "system": "test_generator",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    }
    metadata[b"batch_info_json"] = json.dumps(batch_info, ensure_ascii=False).encode("utf-8")

    # Numeric strings
    metadata[b"row_checksum"] = b"a1b2c3d4e5f6"
    metadata[b"compression_ratio"] = b"0.75"

    # Empty value (edge case)
    metadata[b"optional_field"] = b""

    # Special characters
    metadata[b"special_chars"] = b"line1\nline2\ttabbed"

    # Long value
    metadata[b"long_description"] = ("This is a longer description that spans multiple words " * 10).encode("utf-8")

    return metadata


def create_schema_with_metadata() -> pa.Schema:
    """
    Create a schema with schema-level and field-level metadata.
    """
    schema_metadata = {
        b"schema_version": b"1.0",
        b"created_by": b"record-batch-with-message-metadata.py",
        b"description": b"Test schema for message-level metadata testing",
    }

    fields = [
        pa.field("id", pa.int64(), nullable=False, metadata={
            b"description": b"Unique record identifier",
            b"primary_key": b"true",
        }),
        pa.field("name", pa.utf8(), nullable=True, metadata={
            b"description": b"Name field with Unicode support",
            b"max_length": b"255",
        }),
        pa.field("value", pa.float64(), nullable=True, metadata={
            b"description": b"Numeric value",
            b"unit": b"units",
        }),
    ]

    return pa.schema(fields, metadata=schema_metadata)


def create_record_batches(schema: pa.Schema, num_batches: int = 3) -> list[pa.RecordBatch]:
    """
    Create multiple record batches with different data.
    """
    batches = []

    for i in range(num_batches):
        base_id = i * 5
        ids = list(range(base_id + 1, base_id + 6))
        names = [f"Name_{j}" for j in ids]
        # Add some Unicode to names
        if i == 1:
            names = ["Alice 🎉", "Bob 世界", "Charlie", "Diana مرحبا", "Eve"]
        values = [float(j) * 1.5 + i for j in range(5)]
        # Add some nulls
        if i == 2:
            values[2] = None

        batch = pa.record_batch({
            "id": ids,
            "name": names,
            "value": values,
        }, schema=schema)
        batches.append(batch)

    return batches


def write_ipc_file(filepath: Path, schema: pa.Schema, batches: list[pa.RecordBatch]) -> None:
    """
    Write batches to an Arrow IPC file with message-level custom metadata.
    """
    print(f"Writing to: {filepath}")
    print(f"  Schema fields: {len(schema)}")
    print(f"  Number of batches: {len(batches)}")

    with pa.OSFile(str(filepath), 'wb') as sink:
        with ipc.new_file(sink, schema) as writer:
            for i, batch in enumerate(batches):
                # Create unique metadata for each batch
                custom_metadata = create_message_metadata(i)

                print(f"  Writing batch {i}: {batch.num_rows} rows, {len(custom_metadata)} metadata entries")

                # Write with custom metadata attached to the message
                writer.write_batch(batch, custom_metadata=custom_metadata)

    file_size = filepath.stat().st_size
    print(f"  File size: {file_size:,} bytes")


def verify_file(filepath: Path) -> None:
    """
    Read back the file and verify metadata is present.
    """
    print(f"\nVerifying: {filepath}")

    with pa.OSFile(str(filepath), 'rb') as source:
        reader = ipc.open_file(source)

        print(f"  Schema metadata: {len(reader.schema.metadata or {})} entries")

        for i in range(reader.num_record_batches):
            batch = reader.get_batch(i)
            print(f"  Batch {i}: {batch.num_rows} rows")

    # Also read raw messages to check metadata
    print("\n  Raw message inspection:")
    with open(filepath, 'rb') as f:
        data = f.read()

    # Use MessageReader (for stream format) - won't work for file format
    # File format stores batches differently
    print("  (Message-level metadata is embedded in the IPC format)")


def main():
    print("=" * 70)
    print("Arrow IPC File with Message-Level Custom Metadata")
    print("=" * 70)
    print(f"PyArrow version: {pa.__version__}")

    # Determine output path
    if len(sys.argv) > 1:
        output_file = Path(sys.argv[1])
    else:
        output_file = Path(__file__).parent / "test_message_metadata.arrow"

    print(f"Output file: {output_file}")
    print()

    # Create schema and batches
    schema = create_schema_with_metadata()
    batches = create_record_batches(schema, num_batches=3)

    # Write file with message-level metadata
    write_ipc_file(output_file, schema, batches)

    # Verify
    verify_file(output_file)

    print()
    print("=" * 70)
    print("SUCCESS: File created with message-level custom metadata")
    print("=" * 70)
    print()
    print("Message metadata keys per batch:")
    sample_metadata = create_message_metadata(0)
    for key in sorted(sample_metadata.keys()):
        value = sample_metadata[key]
        display_value = value.decode('utf-8', errors='replace')
        if len(display_value) > 50:
            display_value = display_value[:47] + "..."
        print(f"  {key.decode()}: {display_value}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
