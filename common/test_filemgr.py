"""Test FileManager split and reassemble workflow."""

from pathlib import Path
import shutil
import tempfile

import pytest

from common.filemgr import FileManager


@pytest.fixture
def temp_test_dir():
    """Create a temporary test directory and clean up after test."""
    test_dir = Path(tempfile.mkdtemp(prefix="test_filemgr_"))
    yield test_dir
    shutil.rmtree(test_dir, ignore_errors=True)


@pytest.fixture
def temp_file(temp_test_dir):
    """Create a temporary test file with content."""
    test_file = temp_test_dir / "test_data.txt"
    test_content = "Test content " * 10000
    test_file.write_text(test_content)
    return test_file, test_content


def test_split_and_reassemble(temp_file, temp_test_dir):
    """Test the split and reassemble workflow."""
    test_file, test_content = temp_file
    output_dir = temp_test_dir / "output"
    output_dir.mkdir()

    # Test the split operation
    fm = FileManager(
        file_path=test_file,
        chunk_size=5000,  # Small chunks for testing
        sentinel_filename="test_chunks.txt",
    )

    chunk_count = fm.compress_and_split(output_dir=output_dir)
    assert chunk_count > 0, "Should create at least one chunk"

    # Verify chunk files exist
    chunk_files = list(output_dir.glob("test_data.txt.zip.*"))
    assert len(chunk_files) == chunk_count, (
        "Number of chunk files should match chunk count"
    )

    # Verify sentinel file exists
    sentinel_file = output_dir / "test_chunks.txt"
    assert sentinel_file.exists(), "Sentinel file should exist"
    assert int(sentinel_file.read_text()) == chunk_count, (
        "Sentinel should contain chunk count"
    )

    # Test reassemble
    reassembled_file = output_dir / "test_data_reassembled.txt"
    reassembled_path = fm.reassemble(output_path=reassembled_file, input_dir=output_dir)

    # Verify reassembled file
    assert reassembled_path.exists(), "Reassembled file should exist"
    reassembled_content = reassembled_path.read_text()
    assert reassembled_content == test_content, "Content should match original"


def test_compress_and_split(temp_test_dir):
    """Test the compress_and_split operation."""
    test_file = temp_test_dir / "test_file.txt"
    test_file.write_text("Test content " * 5000)

    output_dir = temp_test_dir / "output"
    output_dir.mkdir()

    fm = FileManager(
        file_path=test_file,
        chunk_size=8000,
        sentinel_filename="chunk_count.txt",
    )

    chunk_count = fm.compress_and_split(output_dir=output_dir, delete_original=True)

    # Verify original file was deleted
    assert not test_file.exists(), (
        "Original file should be deleted when delete_original=True"
    )

    # Verify chunks were created
    chunk_files = sorted(output_dir.glob("test_file.txt.zip.*"))
    assert len(chunk_files) == chunk_count, "All chunks should be created"


def test_temp_dir_context_manager(temp_test_dir):
    """Test temporary working directory feature with context manager."""
    test_file = temp_test_dir / "test_file.txt"
    test_file.write_text("Hello World" * 1000)

    # Test with use_temp_dir=True
    with FileManager(test_file, use_temp_dir=True) as fm:
        assert fm.use_temp_dir is True, "use_temp_dir should be True"
        work_dir = fm._get_work_dir()
        assert work_dir.exists(), "Work directory should exist"
        temp_dir_path = fm.temp_dir
        assert temp_dir_path is not None, "Temp dir should be created"

    # After context exit, temp dir should be cleaned up
    assert fm.temp_dir is None, "Temp dir should be None after context exit"
    assert not temp_dir_path.exists(), "Temp dir should be deleted after context exit"


def test_temp_dir_without_context_manager(temp_test_dir):
    """Test temporary working directory without context manager."""
    test_file = temp_test_dir / "test_file.txt"
    test_file.write_text("Hello World" * 1000)

    fm = FileManager(test_file, use_temp_dir=True)
    work_dir = fm._get_work_dir()
    assert work_dir.exists(), "Work directory should exist"

    temp_dir_path = fm.temp_dir
    assert temp_dir_path is not None, "Temp dir should be created"

    # Manually cleanup
    fm.cleanup_temp_dir()
    assert fm.temp_dir is None, "Temp dir should be None after cleanup"
    assert not temp_dir_path.exists(), "Temp dir should be deleted after cleanup"


def test_no_temp_dir(temp_test_dir):
    """Test FileManager with use_temp_dir=False."""
    test_file = temp_test_dir / "test_file.txt"
    test_file.write_text("Hello World" * 1000)

    fm = FileManager(test_file, use_temp_dir=False)
    work_dir = fm._get_work_dir()
    assert work_dir == Path("."), "Work directory should be current directory"
    assert fm.temp_dir is None, "Temp dir should be None when use_temp_dir=False"
