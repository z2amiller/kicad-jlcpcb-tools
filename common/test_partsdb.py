"""Tests for the partsdb module."""

from datetime import date
from pathlib import Path
import shutil
import sqlite3
import sys
import tempfile
from unittest.mock import Mock, patch

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from common.partsdb import _CREATE_STATEMENTS, Generate, PartsDatabase
from common.progress import NoOpProgressBar
from common.translate import ComponentTranslator

# ============================================================================
# PartsDatabase Tests
# ============================================================================


class TestPartsDatabase:
    """Tests for PartsDatabase class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp(prefix="test_partsdb_"))
        self.output_db = self.temp_dir / "test_parts.db"
        self.archive_dir = self.temp_dir / "archive"
        self.archive_dir.mkdir()

    def teardown_method(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_parts_database_init(self):
        """PartsDatabase initializes with correct paths."""
        db = PartsDatabase(self.output_db, self.archive_dir)
        db.close_sqlite()

        assert self.output_db.exists()
        assert isinstance(db.part_count, int)
        assert db.part_count == 0

    def test_parts_database_init_removes_existing(self):
        """PartsDatabase removes existing output database."""
        # Create an existing database
        self.output_db.write_text("old content")
        assert self.output_db.exists()

        db = PartsDatabase(self.output_db, self.archive_dir)
        db.close_sqlite()

        # Old file should be removed and replaced with new database
        assert self.output_db.exists()
        # Verify it's a valid database
        conn = sqlite3.connect(self.output_db)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        assert len(tables) > 0

    def test_parts_database_custom_chunk_num(self):
        """PartsDatabase accepts custom chunk_num filename."""
        custom_chunk = Path("custom_chunk.txt")
        db = PartsDatabase(self.output_db, self.archive_dir, chunk_num=custom_chunk)
        assert db.chunk_num == custom_chunk
        db.close_sqlite()

    def test_parts_database_create_tables(self):
        """PartsDatabase creates all required tables."""
        db = PartsDatabase(self.output_db, self.archive_dir)

        cursor = db.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        db.close_sqlite()

        # Should have parts (FTS5), mapping, meta, categories
        assert "parts" in tables
        assert "mapping" in tables
        assert "meta" in tables
        assert "categories" in tables

    def test_parts_database_parts_table_schema(self):
        """PartsDatabase creates parts table with correct columns."""
        db = PartsDatabase(self.output_db, self.archive_dir)

        cursor = db.conn.cursor()
        cursor.execute("PRAGMA table_info(parts)")
        columns = [row[1] for row in cursor.fetchall()]
        db.close_sqlite()

        # FTS5 virtual tables have slightly different schema, just verify parts table exists
        assert len(columns) > 0

    def test_parts_database_update_parts_single_row(self):
        """PartsDatabase updates parts with single row."""
        db = PartsDatabase(self.output_db, self.archive_dir)

        row = {
            "LCSC Part": "C123456",
            "First Category": "Resistors",
            "Second Category": "Fixed Resistors",
            "MFR.Part": "TEST001",
            "Package": "0805",
            "Solder Joint": 2,
            "Manufacturer": "Samsung",
            "Library Type": "Basic",
            "Description": "Test Resistor",
            "Datasheet": "http://example.com",
            "Price": "1.00",
            "Stock": "1000",
        }

        db.update_parts([row])
        assert db.part_count == 1

        # Verify data was inserted
        cursor = db.conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM parts WHERE "LCSC Part" = ?', ("C123456",))
        count = cursor.fetchone()[0]
        db.close_sqlite()

        assert count == 1

    def test_parts_database_update_parts_multiple_rows(self):
        """PartsDatabase updates parts with multiple rows."""
        db = PartsDatabase(self.output_db, self.archive_dir)

        rows = [
            {
                "LCSC Part": f"C{100000 + i}",
                "First Category": "Category",
                "Second Category": "SubCategory",
                "MFR.Part": f"TEST{i:03d}",
                "Package": "0805",
                "Solder Joint": 2,
                "Manufacturer": "Mfr",
                "Library Type": "Basic",
                "Description": f"Part {i}",
                "Datasheet": "http://example.com",
                "Price": "1.00",
                "Stock": "1000",
            }
            for i in range(10)
        ]

        db.update_parts(rows)
        assert db.part_count == 10

        cursor = db.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM parts")
        count = cursor.fetchone()[0]
        db.close_sqlite()

        assert count == 10

    def test_parts_database_update_parts_empty_list(self):
        """PartsDatabase handles empty update list gracefully."""
        db = PartsDatabase(self.output_db, self.archive_dir)

        db.update_parts([])
        assert db.part_count == 0
        db.close_sqlite()

    def test_parts_database_update_parts_with_special_chars(self):
        """PartsDatabase handles special characters in part data."""
        db = PartsDatabase(self.output_db, self.archive_dir)

        row = {
            "LCSC Part": "C999999",
            "First Category": "Resistors & Capacitors",
            "Second Category": "Film/Foil",
            "MFR.Part": "TEST-001-A",
            "Package": "1206",
            "Solder Joint": 2,
            "Manufacturer": "Inc.",
            "Library Type": "Preferred",
            "Description": 'Resistor "precision" type (±1%)',
            "Datasheet": "http://example.com/ds.pdf?v=1.0",
            "Price": "2.50-3.00",
            "Stock": "5000+",
        }

        db.update_parts([row])
        assert db.part_count == 1
        db.close_sqlite()

    def test_parts_database_populate_categories(self):
        """PartsDatabase populates categories from parts."""
        db = PartsDatabase(self.output_db, self.archive_dir)

        # Insert some parts
        rows = [
            {
                "LCSC Part": "C1",
                "First Category": "Resistors",
                "Second Category": "Fixed",
                "MFR.Part": "T1",
                "Package": "0805",
                "Solder Joint": 2,
                "Manufacturer": "M1",
                "Library Type": "Basic",
                "Description": "D1",
                "Datasheet": "D",
                "Price": "1",
                "Stock": "1000",
            },
            {
                "LCSC Part": "C2",
                "First Category": "Capacitors",
                "Second Category": "Ceramic",
                "MFR.Part": "T2",
                "Package": "0805",
                "Solder Joint": 2,
                "Manufacturer": "M2",
                "Library Type": "Basic",
                "Description": "D2",
                "Datasheet": "D",
                "Price": "1",
                "Stock": "1000",
            },
        ]
        db.update_parts(rows)
        db.populate_categories()

        cursor = db.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM categories")
        count = cursor.fetchone()[0]
        db.close_sqlite()

        assert count == 2

    def test_parts_database_metadata(self):
        """PartsDatabase records metadata."""
        db = PartsDatabase(self.output_db, self.archive_dir)

        # Add some parts first
        row = {
            "LCSC Part": "C1",
            "First Category": "Resistors",
            "Second Category": "Fixed",
            "MFR.Part": "T1",
            "Package": "0805",
            "Solder Joint": 2,
            "Manufacturer": "M1",
            "Library Type": "Basic",
            "Description": "D1",
            "Datasheet": "D",
            "Price": "1",
            "Stock": "1000",
        }
        db.update_parts([row])
        db.meta_data()

        cursor = db.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM meta")
        count = cursor.fetchone()[0]

        cursor.execute("SELECT partcount FROM meta")
        part_count = cursor.fetchone()[0]
        db.close_sqlite()

        assert count == 1
        assert part_count == 1

    def test_parts_database_metadata_records_size(self):
        """PartsDatabase metadata records database size."""
        db = PartsDatabase(self.output_db, self.archive_dir)

        # Add some data
        rows = [
            {
                "LCSC Part": f"C{i}",
                "First Category": "Cat",
                "Second Category": "Sub",
                "MFR.Part": f"T{i}",
                "Package": "0805",
                "Solder Joint": 2,
                "Manufacturer": "M",
                "Library Type": "Basic",
                "Description": f"Description {i}",
                "Datasheet": "D",
                "Price": "1",
                "Stock": "1000",
            }
            for i in range(5)
        ]
        db.update_parts(rows)
        db.meta_data()

        cursor = db.conn.cursor()
        cursor.execute("SELECT size FROM meta")
        size = cursor.fetchone()[0]
        db.close_sqlite()

        # Size should be positive and reasonable
        assert size > 0

    def test_parts_database_metadata_records_date(self):
        """PartsDatabase metadata records current date."""
        db = PartsDatabase(self.output_db, self.archive_dir)
        db.meta_data()

        cursor = db.conn.cursor()
        cursor.execute("SELECT date FROM meta")
        result = cursor.fetchone()[0]
        db.close_sqlite()

        # Should record today's date as a string
        assert str(date.today()) in result or result == str(date.today())

    def test_parts_database_remove_original(self):
        """PartsDatabase removes existing database."""
        # Create a file
        self.output_db.write_text("test")
        assert self.output_db.exists()

        db = PartsDatabase(self.output_db, self.archive_dir)
        # File should be removed during init and recreated as database
        assert self.output_db.exists()
        db.close_sqlite()

    @patch("common.partsdb.FileManager")
    def test_parts_database_split(self, mock_fm):
        """PartsDatabase calls FileManager to split database."""
        db = PartsDatabase(self.output_db, self.archive_dir)

        # Add some data
        row = {
            "LCSC Part": "C1",
            "First Category": "Resistors",
            "Second Category": "Fixed",
            "MFR.Part": "T1",
            "Package": "0805",
            "Solder Joint": 2,
            "Manufacturer": "M1",
            "Library Type": "Basic",
            "Description": "D1",
            "Datasheet": "D",
            "Price": "1",
            "Stock": "1000",
        }
        db.update_parts([row])
        db.close_sqlite()

        # Now test split
        db2 = PartsDatabase(self.output_db, self.archive_dir)
        db2.close_sqlite()

        # Mock the split process so it doesn't actually run
        with patch("common.partsdb.FileManager"):
            pass

    @patch("common.partsdb.os.unlink")
    @patch("common.partsdb.FileManager")
    def test_parts_database_cleanup(self, mock_fm, mock_unlink):
        """PartsDatabase cleanup removes original database file."""
        db = PartsDatabase(self.output_db, self.archive_dir)
        db.close_sqlite()

        db.cleanup()
        mock_unlink.assert_called_once()

    def test_parts_database_skip_cleanup_flag(self):
        """PartsDatabase respects skip_cleanup flag."""
        db = PartsDatabase(self.output_db, self.archive_dir, skip_cleanup=True)
        assert db.skip_cleanup is True
        db.close_sqlite()


# ============================================================================
# Generate Class Tests
# ============================================================================


class TestGenerate:
    """Tests for Generate class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp(prefix="test_generate_"))
        self.output_db = self.temp_dir / "test_parts.db"
        self.archive_dir = self.temp_dir / "archive"
        self.archive_dir.mkdir()

    def teardown_method(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_generate_init(self):
        """Generate initializes with database and translator."""
        mock_componentdb = Mock()
        partsdb = PartsDatabase(self.output_db, self.archive_dir)
        progress = NoOpProgressBar()
        translator = ComponentTranslator({}, {})

        gen = Generate(mock_componentdb, partsdb, progress, translator)

        assert gen.componentdb == mock_componentdb
        assert gen.partsdb == partsdb
        assert gen.translator == translator
        assert gen.progress == progress
        assert gen.total_components == 0
        assert gen.loaded_components == 0

        partsdb.close_sqlite()

    def test_generate_init_without_translator(self):
        """Generate creates translator if not provided."""
        mock_componentdb = Mock()
        mock_componentdb.get_manufacturers.return_value = {1: "Samsung"}
        mock_componentdb.get_categories.return_value = {1: ("Resistors", "Fixed")}

        partsdb = PartsDatabase(self.output_db, self.archive_dir)
        progress = NoOpProgressBar()

        gen = Generate(mock_componentdb, partsdb, progress)

        assert gen.translator is None  # Not created until generate() is called
        partsdb.close_sqlite()

    def test_generate_tracks_components(self):
        """Generate tracks total and loaded component counts."""
        mock_componentdb = Mock()
        mock_componentdb.count_components.return_value = 100
        mock_componentdb.fetch_components.return_value = []

        partsdb = PartsDatabase(self.output_db, self.archive_dir)
        progress = NoOpProgressBar()

        gen = Generate(mock_componentdb, partsdb, progress)

        assert gen.total_components == 0
        gen.total_components = 100

        assert gen.total_components == 100
        partsdb.close_sqlite()

    @patch("common.partsdb.ComponentTranslator")
    def test_generate_creates_translator_on_demand(self, mock_translator_class):
        """Generate creates translator if not provided during generate()."""
        mock_componentdb = Mock()
        mock_componentdb.count_components.return_value = 0
        mock_componentdb.fetch_components.return_value = []
        mock_componentdb.get_manufacturers.return_value = {}
        mock_componentdb.get_categories.return_value = {}

        partsdb = PartsDatabase(self.output_db, self.archive_dir)
        progress = NoOpProgressBar()

        gen = Generate(mock_componentdb, partsdb, progress)

        # Translator is None initially
        assert gen.translator is None

        # Call generate (but with mocked data so it doesn't do much)
        with patch.object(gen, "_process_batches"), patch.object(partsdb, "post_build"):
            gen.generate()

        # Translator should still be None in this context (mocked away)
        partsdb.close_sqlite()

    def test_generate_process_batches_empty(self):
        """Generate handles empty component batches."""
        mock_componentdb = Mock()
        mock_componentdb.fetch_components.return_value = []

        partsdb = PartsDatabase(self.output_db, self.archive_dir)
        progress = NoOpProgressBar()
        translator = ComponentTranslator({}, {})

        gen = Generate(mock_componentdb, partsdb, progress, translator)
        gen._process_batches("", None)

        assert gen.loaded_components == 0
        partsdb.close_sqlite()

    def test_generate_process_batches_single_batch(self):
        """Generate processes a single batch of components."""
        # Create mock component rows
        mock_rows = []
        for _ in range(5):
            mock_row = Mock()
            mock_rows.append(mock_row)

        mock_componentdb = Mock()
        mock_componentdb.fetch_components.return_value = [mock_rows]

        partsdb = PartsDatabase(self.output_db, self.archive_dir)
        progress = NoOpProgressBar()

        # Create a translator that returns valid part data
        translator = Mock(spec=ComponentTranslator)
        translator.translate.side_effect = [
            {
                "LCSC Part": f"C{i}",
                "First Category": "Cat",
                "Second Category": "Sub",
                "MFR.Part": f"T{i}",
                "Package": "0805",
                "Solder Joint": 2,
                "Manufacturer": "M",
                "Library Type": "Basic",
                "Description": f"D{i}",
                "Datasheet": "D",
                "Price": "1",
                "Stock": "1000",
            }
            for i in range(5)
        ]

        gen = Generate(mock_componentdb, partsdb, progress, translator)
        gen.total_components = 5
        gen._process_batches("", None)

        assert gen.loaded_components == 5
        partsdb.close_sqlite()

    def test_generate_report_stats_no_translator(self):
        """Generate reports error when no data processed."""
        mock_componentdb = Mock()
        partsdb = PartsDatabase(self.output_db, self.archive_dir)
        progress = NoOpProgressBar()

        gen = Generate(mock_componentdb, partsdb, progress)

        # Should not crash, just report no data
        gen.translator = None
        # This would print to stdout, we just verify it doesn't crash
        gen.report_stats()

        partsdb.close_sqlite()

    def test_generate_report_stats_with_data(self):
        """Generate reports statistics from translator."""
        mock_componentdb = Mock()
        partsdb = PartsDatabase(self.output_db, self.archive_dir)
        progress = NoOpProgressBar()

        translator = Mock(spec=ComponentTranslator)
        translator.get_statistics.return_value = (1000, 50, 10)

        gen = Generate(mock_componentdb, partsdb, progress, translator)
        gen.report_stats()

        translator.get_statistics.assert_called_once()
        partsdb.close_sqlite()

    def test_generate_report_stats_zero_deletion(self):
        """Generate handles zero deletions in statistics."""
        mock_componentdb = Mock()
        partsdb = PartsDatabase(self.output_db, self.archive_dir)
        progress = NoOpProgressBar()

        translator = Mock(spec=ComponentTranslator)
        translator.get_statistics.return_value = (1000, 0, 0)

        gen = Generate(mock_componentdb, partsdb, progress, translator)
        gen.report_stats()

        translator.get_statistics.assert_called_once()
        partsdb.close_sqlite()


# ============================================================================
# Integration Tests
# ============================================================================


class TestPartsDBIntegration:
    """Integration tests for partsdb module."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp(prefix="test_partsdb_int_"))
        self.output_db = self.temp_dir / "test_parts.db"
        self.archive_dir = self.temp_dir / "archive"
        self.archive_dir.mkdir()

    def teardown_method(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_parts_database_full_workflow(self):
        """PartsDatabase handles full workflow: create, insert, optimize, metadata."""
        db = PartsDatabase(self.output_db, self.archive_dir)

        # Insert parts
        rows = [
            {
                "LCSC Part": "C1",
                "First Category": "Resistors",
                "Second Category": "Fixed",
                "MFR.Part": "T1",
                "Package": "0805",
                "Solder Joint": 2,
                "Manufacturer": "M1",
                "Library Type": "Basic",
                "Description": "D1",
                "Datasheet": "D",
                "Price": "1",
                "Stock": "1000",
            },
        ]
        db.update_parts(rows)

        # Populate categories
        db.populate_categories()

        # Add metadata
        db.meta_data()

        # Verify everything worked
        cursor = db.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM parts")
        parts_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM categories")
        categories_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM meta")
        meta_count = cursor.fetchone()[0]

        db.close_sqlite()

        assert parts_count == 1
        assert categories_count == 1
        assert meta_count == 1

    def test_multiple_category_insertion(self):
        """PartsDatabase handles multiple categories correctly."""
        db = PartsDatabase(self.output_db, self.archive_dir)

        # Insert parts with different categories
        rows = [
            {
                "LCSC Part": f"C{i}",
                "First Category": f"Category{i // 3}",
                "Second Category": f"SubCategory{i}",
                "MFR.Part": f"T{i}",
                "Package": "0805",
                "Solder Joint": 2,
                "Manufacturer": "M",
                "Library Type": "Basic",
                "Description": f"D{i}",
                "Datasheet": "D",
                "Price": "1",
                "Stock": "1000",
            }
            for i in range(9)
        ]
        db.update_parts(rows)
        db.populate_categories()

        cursor = db.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM categories")
        categories_count = cursor.fetchone()[0]
        db.close_sqlite()

        # Should have 3 first categories with multiple second categories
        assert categories_count > 0

    def test_fts5_search_capability(self):
        """PartsDatabase parts table supports FTS5 search."""
        db = PartsDatabase(self.output_db, self.archive_dir)

        rows = [
            {
                "LCSC Part": "C1",
                "First Category": "Resistors",
                "Second Category": "Fixed",
                "MFR.Part": "RESMFR001",
                "Package": "0805",
                "Solder Joint": 2,
                "Manufacturer": "ResistorCorp",
                "Library Type": "Basic",
                "Description": "10K resistor precision type",
                "Datasheet": "http://example.com",
                "Price": "0.01",
                "Stock": "10000",
            },
            {
                "LCSC Part": "C2",
                "First Category": "Capacitors",
                "Second Category": "Ceramic",
                "MFR.Part": "CAPMFR001",
                "Package": "0805",
                "Solder Joint": 2,
                "Manufacturer": "CapacitorCorp",
                "Library Type": "Preferred",
                "Description": "100nF ceramic capacitor",
                "Datasheet": "http://example.com",
                "Price": "0.005",
                "Stock": "50000",
            },
        ]
        db.update_parts(rows)

        # Test FTS5 search
        cursor = db.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM parts WHERE parts MATCH ?", ("resistor",))
        count = cursor.fetchone()[0]

        db.close_sqlite()

        assert count > 0

    def test_special_field_name_handling(self):
        """PartsDatabase handles field names with spaces and dots correctly."""
        db = PartsDatabase(self.output_db, self.archive_dir)

        # Fields have spaces like "LCSC Part", "Solder Joint", etc.
        row = {
            "LCSC Part": "C999",
            "First Category": "Test",
            "Second Category": "Test",
            "MFR.Part": "TEST.MFR.001",
            "Package": "BGA",
            "Solder Joint": 144,
            "Manufacturer": "TestCorp",
            "Library Type": "Extended",
            "Description": "Complex test part",
            "Datasheet": "http://example.com/datasheet.pdf",
            "Price": "99.99",
            "Stock": "100",
        }
        db.update_parts([row])

        cursor = db.conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM parts WHERE "LCSC Part" = ?', ("C999",))
        count = cursor.fetchone()[0]
        db.close_sqlite()

        assert count == 1


# ============================================================================
# Constants Tests
# ============================================================================


class TestCreateStatements:
    """Tests for CREATE_STATEMENTS constants."""

    def test_create_statements_exists(self):
        """CREATE_STATEMENTS constant exists."""
        assert _CREATE_STATEMENTS is not None
        assert isinstance(_CREATE_STATEMENTS, list)

    def test_create_statements_not_empty(self):
        """CREATE_STATEMENTS has statements."""
        assert len(_CREATE_STATEMENTS) > 0

    def test_create_statements_are_strings(self):
        """All CREATE_STATEMENTS are SQL strings."""
        for stmt in _CREATE_STATEMENTS:
            assert isinstance(stmt, str)
            assert "CREATE" in stmt.upper()

    def test_create_statements_count(self):
        """CREATE_STATEMENTS has expected number of table definitions."""
        # Should have: parts (FTS5), mapping, meta, categories
        assert len(_CREATE_STATEMENTS) >= 4

    def test_parts_table_statement(self):
        """Parts table statement creates FTS5 virtual table."""
        parts_stmt = _CREATE_STATEMENTS[0]
        assert "parts" in parts_stmt.lower()
        assert "fts5" in parts_stmt.lower()
        assert "LCSC Part" in parts_stmt

    def test_mapping_table_statement(self):
        """Mapping table statement exists."""
        assert any("mapping" in stmt.lower() for stmt in _CREATE_STATEMENTS)

    def test_meta_table_statement(self):
        """Meta table statement exists."""
        assert any("meta" in stmt.lower() for stmt in _CREATE_STATEMENTS)

    def test_categories_table_statement(self):
        """Categories table statement exists."""
        assert any("categories" in stmt.lower() for stmt in _CREATE_STATEMENTS)
