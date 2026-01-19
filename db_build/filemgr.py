#!/usr/bin/env python3

"""File management utilities for splitting, reassembling, and downloading files.

This module provides classes to handle:
- Splitting large files into chunks for GitHub compatibility
- Reassembling split files using the split_file_reader package
- Downloading files from GitHub archives
"""

import argparse
from collections.abc import Callable, Generator
import glob
from pathlib import Path
import shutil
import tempfile
from typing import Any
import zipfile

import requests
from split_file_reader.split_file_reader import SplitFileReader
from split_file_reader.split_file_writer import SplitFileWriter

from .progress import NestedProgressBar, NoOpProgressBar, TqdmNestedProgressBar


class FileManager:
    """Manage file splitting, reassembly, and downloads."""

    def __init__(
        self,
        file_path: Path | str,
        chunk_size: int = 80000000,  # 80 MB default
        sentinel_filename: str = "chunk_num.txt",
        compressed_output_file: str | None = None,
        use_temp_dir: bool = False,
    ):
        """Initialize FileManager.

        Args:
            file_path: Path to the file to zip and split.
            chunk_size: Size of each split chunk in bytes (default 80MB for GitHub)
            sentinel_filename: Name of the sentinel file that tracks chunk count.
            compressed_output_file: Path for compressed output file
                                    (defaults to file_path.zip).  May end up being
                                    the prefix of the split files.
            use_temp_dir: If True, create a temporary working directory for
                         intermediate files. Useful for large operations.

        """
        self.file_path = Path(file_path)
        self.chunk_size = chunk_size
        self.sentinel_filename = Path(sentinel_filename)
        self.compressed_output_file = (
            Path(compressed_output_file)
            if compressed_output_file
            else Path(f"{self.file_path}.zip")
        )
        self.use_temp_dir = use_temp_dir
        self.temp_dir: Path | None = None

    def _get_work_dir(self) -> Path:
        """Get the working directory, creating temp dir if needed.

        Returns:
            Path: The working directory (either temp or current).

        """
        if self.use_temp_dir:
            if self.temp_dir is None:
                self.temp_dir = Path(tempfile.mkdtemp(prefix="filemanager_"))
                print(f"Created temporary working directory: {self.temp_dir}")  # noqa: T201
            return self.temp_dir
        return Path(".")

    def cleanup_temp_dir(self) -> None:
        """Clean up the temporary working directory if it exists.

        This method should be called when finished with the FileManager
        to clean up temporary files.

        """
        if self.temp_dir and self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
            print(f"Cleaned up temporary directory: {self.temp_dir}")  # noqa: T201
            self.temp_dir = None

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit, ensuring temp dir cleanup."""
        self.cleanup_temp_dir()
        return False

    def split(self) -> int:
        """Split the file into chunks, creating a sentinel file with chunk count.

        This method maintains compatibility with Generate.split() output format.
        It splits the file at self.file_path into numbered chunks (e.g., file.zip.001, .002, etc.)
        and creates a sentinel file indicating the number of chunks.

        Returns:
            int: The number of chunks created

        Raises:
            FileNotFoundError: If the file to split does not exist.

        """
        if not self.file_path.exists():
            raise FileNotFoundError(f"File to split not found: {self.file_path}")

        class SplitTracker:
            """Custom SplitFileWriter to track chunk count."""

            def __init__(self, file_prefix: str):
                self.file_prefix = file_prefix
                self.files = []

            def gen_split(self) -> Generator[Any, Any, None]:
                while True:
                    name = f"{self.file_prefix}{len(self.files):03d}"
                    with open(name, "wb") as output_file:
                        self.files.append(name)
                        yield output_file

            def get_chunk_count(self) -> int:
                return len(self.files)

        work_dir = self._get_work_dir()
        print(f"Chunking {self.file_path}")  # noqa: T201

        # Build output file path in working directory
        output_prefix = work_dir / self.compressed_output_file.name
        tracker = SplitTracker(str(output_prefix) + ".")
        with (
            SplitFileWriter(tracker.gen_split(), self.chunk_size) as writer,
            zipfile.ZipFile(
                file=writer, mode="w", compression=zipfile.ZIP_DEFLATED
            ) as zip_writer,
        ):
            zip_writer.write(self.file_path, arcname=self.file_path.name)

        # Create sentinel file indicating the number of chunks
        sentinel_path = work_dir / self.sentinel_filename
        with open(sentinel_path, "w", encoding="utf-8") as f:  # noqa: T201
            f.write(str(tracker.get_chunk_count()))

        print(  # noqa: T201
            f"Created {tracker.get_chunk_count()} chunks with sentinel file: {sentinel_path}"
        )
        return tracker.get_chunk_count()

    def reassemble(
        self,
        output_path: Path | str | None = None,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> Path:
        """Reassemble split chunks back into the original file.

        Uses split_file_reader to intelligently handle reassembly by reading
        the chunk metadata and combining them in the correct order.

        Args:
            output_path: Path for the reassembled file (defaults to self.file_path)
            progress_callback: Optional callback function(bytes_read, total_bytes)
                             for progress reporting

        Returns:
            Path: Path to the reassembled file

        Raises:
            FileNotFoundError: If chunk files are missing
            ValueError: If sentinel file cannot be read or is invalid.

        """
        if output_path is None:
            output_path = self.file_path

        output_path = Path(output_path)
        files = glob.glob(f"{self.compressed_output_file}*")
        print(
            f"Matching chunks with prefix: {self.compressed_output_file} got {list(files)}"
        )  # noqa: T201
        print(f"Reassembling {len(files)} chunks into {output_path}")  # noqa: T201
        with (
            SplitFileReader(sorted(files), "r") as sfr,
            zipfile.ZipFile(sfr, "r") as zip_reader,  # type: ignore
        ):
            zip_reader.extractall(path=output_path.parent)

        print(f"Successfully reassembled file: {output_path}")  # noqa: T201
        return output_path

    def download_from_github(
        self,
        github_url: str,
        output_dir: Path | str | None = None,
        progress_manager: NestedProgressBar | None = None,
    ) -> Path:
        """Download split file chunks from a GitHub release or archive.

        Args:
            github_url: Base URL to the GitHub archive (without filename or chunk extension)
            output_dir: Directory to download chunks into (defaults to current directory)
            progress_manager: Optional NestedProgressBar instance for progress reporting.

        Returns:
            Path: Path to the downloaded file.

        Raises:
            FileNotFoundError: If sentinel file cannot be found at the remote location
            OSError: If download fails.

        """
        if progress_manager is None:
            progress_manager = NoOpProgressBar()

        if output_dir is None:
            output_dir = Path(".")
        else:
            output_dir = Path(output_dir)

        # Use temp dir for downloads if configured, otherwise use output_dir
        download_dir = self._get_work_dir() if self.use_temp_dir else output_dir
        download_dir.mkdir(parents=True, exist_ok=True)

        # Download sentinel file first to determine chunk count
        sentinel_url = f"{github_url}/{self.sentinel_filename}"
        sentinel_local = download_dir / self.sentinel_filename

        print(f"Downloading sentinel file from {sentinel_url}")  # noqa: T201
        try:
            response = requests.get(sentinel_url, allow_redirects=True, timeout=300)
            response.raise_for_status()
            with open(sentinel_local, "w", encoding="utf-8") as f:
                f.write(response.text)
        except requests.RequestException as e:
            raise FileNotFoundError(
                f"Could not download sentinel file from {sentinel_url}: {e}"
            ) from e

        # Read sentinel to get chunk count
        try:
            with open(sentinel_local, encoding="utf-8") as f:
                chunk_count = int(f.read().strip())
        except ValueError as e:
            raise ValueError(f"Invalid sentinel file format at {sentinel_local}") from e

        print(f"Downloading {chunk_count} chunks from {github_url}")  # noqa: T201

        with progress_manager.outer(
            chunk_count,
            description=f"Downloading {self.compressed_output_file.name}    ",
        ) as outer_pbar:  # type: ignore
            for i in range(1, chunk_count + 1):
                chunk_filename = f"{self.compressed_output_file.name}.{i:03d}"
                chunk_url = f"{github_url}/{chunk_filename}"
                chunk_local = download_dir / chunk_filename

                try:
                    with progress_manager.inner(
                        description=f"Downloading {chunk_filename}"
                    ) as inner_pbar:  # type: ignore
                        response = requests.get(
                            chunk_url,
                            allow_redirects=True,
                            stream=True,
                            timeout=300,
                        )
                        response.raise_for_status()

                        # Get file size from headers
                        file_size = int(response.headers.get("Content-Length", 0))
                        inner_pbar.set_total(file_size)

                        # Download in chunks
                        with open(chunk_local, "wb") as f:
                            for chunk_data in response.iter_content(chunk_size=4096):
                                if chunk_data:
                                    f.write(chunk_data)
                                    inner_pbar.update(len(chunk_data))

                    outer_pbar.update()
                except requests.RequestException as e:
                    raise OSError(
                        f"Failed to download chunk {chunk_filename} from {chunk_url}: {e}"
                    ) from e

        # If using temp dir, copy files to output_dir
        if self.use_temp_dir and download_dir != output_dir:
            output_dir.mkdir(parents=True, exist_ok=True)
            for file in download_dir.glob(f"{self.compressed_output_file.name}*"):
                shutil.copy2(file, output_dir / file.name)
            print(f"Copied downloaded files to {output_dir}")  # noqa: T201

        return output_dir


def main() -> None:
    """Download parts-fts5.db files from GitHub."""

    parser = argparse.ArgumentParser(
        description="Download parts-fts5.db split files from GitHub release"
    )
    parser.add_argument(
        "-u",
        "--url",
        type=str,
        default="https://bouni.github.io/kicad-jlcpcb-tools/",
        help="Base URL to GitHub release directory containing split files",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("."),
        help="Output directory for downloaded files (default: current directory)",
    )
    parser.add_argument(
        "--reassemble",
        action="store_true",
        default=True,
        help="Reassemble files after downloading",
    )
    parser.add_argument(
        "--output-file",
        type=Path,
        help="Output filename for reassembled file (only with --reassemble)",
    )

    args = parser.parse_args()

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Downloading parts-fts5.db files from {args.url}")  # noqa: T201
    print(f"Output directory: {output_dir.absolute()}")  # noqa: T201

    try:
        manager = FileManager(
            file_path="parts-fts5.db", sentinel_filename="chunk_num_fts5.txt"
        )

        # Download the split files
        manager.download_from_github(
            args.url, output_dir=output_dir, progress_manager=TqdmNestedProgressBar()
        )
        print("✓ Download complete!")  # noqa: T201

        # Reassemble if requested
        if args.reassemble:
            output_file = args.output_file or (output_dir / "parts-fts5.db.zip")
            print(f"\nReassembling to {output_file}...")  # noqa: T201
            manager.reassemble()
            print("✓ Reassembly complete!")  # noqa: T201

            # Verify the file
            if output_file.exists():
                file_size = output_file.stat().st_size
                print(
                    f"\n✓ Success! Reassembled file: {output_file.name} ({file_size:,} bytes)"
                )  # noqa: T201
            else:
                print(f"\n✗ Error: {output_file.name} not found")  # noqa: T201

    except FileNotFoundError as e:
        print(f"\n✗ File not found: {e}")  # noqa: T201
    except ValueError as e:
        print(f"\n✗ Invalid sentinel file: {e}")  # noqa: T201
    except OSError as e:
        print(f"\n✗ Download error: {e}")  # noqa: T201
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")  # noqa: T201


if __name__ == "__main__":
    main()
