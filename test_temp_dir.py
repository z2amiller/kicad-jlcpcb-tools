#!/usr/bin/env python3
"""Test temporary working directory feature."""

from db_build.filemgr import FileManager
from pathlib import Path

# Create a test file
test_file = Path("test_file.txt")
test_file.write_text("Hello World" * 1000)

print("Test 1: FileManager with use_temp_dir=True")
with FileManager(test_file, use_temp_dir=True) as fm:
    print(f"  Temp dir enabled: {fm.use_temp_dir}")
    print(f"  Before work: temp_dir={fm.temp_dir}")
    work_dir = fm._get_work_dir()
    print(f"  After _get_work_dir(): work_dir={work_dir}")
    print(f"  Temp dir exists: {work_dir.exists()}")
print(f"  After context exit: temp_dir cleaned up: {fm.temp_dir is None}")

print("\nTest 2: FileManager with use_temp_dir=False")
fm2 = FileManager(test_file, use_temp_dir=False)
work_dir2 = fm2._get_work_dir()
print(f"  Work dir: {work_dir2}")
print(f"  Temp dir: {fm2.temp_dir}")

# Clean up
test_file.unlink()
print("\nAll tests passed!")
