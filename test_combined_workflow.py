#!/usr/bin/env python3
"""Test combined workflow for split, reassemble, and cleanup."""

from db_build.filemgr import FileManager
from pathlib import Path
import tempfile
import shutil

# Create test data
test_dir = Path(tempfile.mkdtemp(prefix="test_combined_"))
test_file = test_dir / "test_data.txt"
test_file.write_text("Test content " * 10000)  # Create a reasonably sized file

print(f"Test directory: {test_dir}")
print(f"Test file size: {test_file.stat().st_size} bytes")

# Create FileManager instance
fm = FileManager(
    file_path=test_file,
    chunk_size=5000,  # Small chunks for testing
    sentinel_filename="test_chunks.txt",
    compressed_output_file=str(test_dir / "test_data.zip"),
)

# Test the split operation
print("\n=== Testing split operation ===")
chunk_count = fm.split()
print(f"Created {chunk_count} chunks")

# List files after split
chunk_files = list(test_dir.glob("test_data.zip.*"))
print(f"Chunk files: {[f.name for f in chunk_files]}")

# Test reassemble
print("\n=== Testing reassemble operation ===")
# Note: reassemble extracts files based on their archive names
# The file was archived as "test_data.txt", so it will be extracted with that name
reassembled = fm.reassemble(input_dir=test_dir)
print(f"Reassembled method returned: {reassembled}")

# The actual extracted file will be in test_dir with the archive name
extracted_file = (
    test_dir / "test_data.txt"
)  # This was the arcname when creating the zip
print(f"Looking for extracted file at: {extracted_file}")

if extracted_file.exists():
    print(f"Found extracted file: {extracted_file.name}")
    print(f"Reassembled file size: {extracted_file.stat().st_size} bytes")
else:
    print(f"✗ Extracted file not found at {extracted_file}")

# Verify content matches
if test_file.read_text() == extracted_file.read_text():
    print("✓ Content matches!")
else:
    print("✗ Content does NOT match!")

# Test cleanup
print("\n=== Testing cleanup of intermediate files ===")
print(f"Files before cleanup: {sorted([f.name for f in test_dir.iterdir()])}")
fm._cleanup_intermediate_files(test_dir)
print(f"Files after cleanup: {sorted([f.name for f in test_dir.iterdir()])}")

# Clean up test directory
shutil.rmtree(test_dir)
print(f"\nTest directory cleaned up: {not test_dir.exists()}")
