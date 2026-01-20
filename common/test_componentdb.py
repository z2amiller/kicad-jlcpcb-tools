"""Tests for the componentdb module."""

import json
from pathlib import Path
import shutil
import sqlite3
import sys
import tempfile
import time

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from common.componentdb import _CREATE_STATEMENTS, ComponentsDatabase, fixDescription

# ============================================================================
# fixDescription Tests
# ============================================================================


class TestFixDescription:
    """Tests for fixDescription utility function."""

    def test_fix_description_with_empty_description(self):
        """FixDescription extracts description from extra JSON when empty."""
        extra_json = json.dumps({"description": "Extracted description"})
        result = fixDescription("", extra_json)
        assert result == "Extracted description"

    def test_fix_description_with_none_description(self):
        """FixDescription handles None description."""
        extra_json = json.dumps({"description": "Extracted description"})
        result = fixDescription(None, extra_json)
        assert result == "Extracted description"

    def test_fix_description_preserves_existing(self):
        """FixDescription preserves existing non-empty description."""
        extra_json = json.dumps({"description": "Should not use this"})
        result = fixDescription("Original description", extra_json)
        assert result == "Original description"

    def test_fix_description_falls_back_to_describe(self):
        """FixDescription falls back to 'describe' key if 'description' missing."""
        extra_json = json.dumps({"describe": "Describe fallback"})
        result = fixDescription("", extra_json)
        assert result == "Describe fallback"

    def test_fix_description_invalid_json(self):
        """FixDescription handles invalid JSON gracefully."""
        result = fixDescription("", "invalid json")
        assert result == ""

    def test_fix_description_no_description_keys(self):
        """FixDescription returns empty string when no description keys found."""
        extra_json = json.dumps({"other_key": "value"})
        result = fixDescription("", extra_json)
        assert result == ""

    def test_fix_description_empty_extra_json(self):
        """FixDescription handles empty extra JSON."""
        result = fixDescription("", "{}")
        assert result == ""


# ============================================================================
# ComponentsDatabase Tests
# ============================================================================


def _insert_component(
    db,
    lcsc,
    category_id=1,
    mfr="MFR",
    package="0805",
    joints=2,
    manufacturer_id=1,
    basic=1,
    description="Component",
    datasheet="http://example.com",
    stock=100,
    price="[]",
    last_update=None,
    extra=None,
):
    """Insert a component into the test database."""
    if last_update is None:
        last_update = int(time.time())

    params = [
        lcsc,
        category_id,
        mfr,
        package,
        joints,
        manufacturer_id,
        basic,
        description,
        datasheet,
        stock,
        price,
        last_update,
    ]

    if extra is not None:
        db.conn.execute(
            """INSERT INTO components
            (lcsc, category_id, mfr, package, joints, manufacturer_id,
             basic, description, datasheet, stock, price, last_update, extra)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            params + [extra],
        )
    else:
        db.conn.execute(
            """INSERT INTO components
            (lcsc, category_id, mfr, package, joints, manufacturer_id,
             basic, description, datasheet, stock, price, last_update)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            params,
        )


class TestComponentsDatabase:
    """Tests for ComponentsDatabase class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp(prefix="test_componentdb_"))
        self.db_path = self.temp_dir / "test_components.db"

    def teardown_method(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_components_database_init(self):
        """ComponentsDatabase initializes with database file."""
        db = ComponentsDatabase(str(self.db_path))
        assert self.db_path.exists()
        db.close()

    def test_components_database_creates_tables(self):
        """ComponentsDatabase creates all required tables."""
        db = ComponentsDatabase(str(self.db_path))

        cursor = db.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        db.close()

        assert "components" in tables
        assert "manufacturers" in tables
        assert "categories" in tables

    def test_components_database_creates_indexes(self):
        """ComponentsDatabase creates all required indexes."""
        db = ComponentsDatabase(str(self.db_path))

        cursor = db.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
        indexes = [row[0] for row in cursor.fetchall()]
        db.close()

        assert "components_category" in indexes
        assert "components_manufacturer" in indexes

    def test_components_database_row_factory(self):
        """ComponentsDatabase uses row_factory for sqlite3.Row."""
        db = ComponentsDatabase(str(self.db_path))
        assert db.conn.row_factory == sqlite3.Row
        db.close()

    def test_components_database_has_fix_description_udf(self):
        """ComponentsDatabase registers maybeFixDescription UDF."""
        db = ComponentsDatabase(str(self.db_path))

        # Test by using the function in a query
        cursor = db.conn.cursor()
        result = cursor.execute(
            "SELECT maybeFixDescription('', ?)",
            (json.dumps({"description": "Test"}),),
        ).fetchone()
        db.close()

        assert result[0] == "Test"

    def test_manufacturer_id_new_manufacturer(self):
        """ComponentsDatabase manufacturerId inserts new manufacturer."""
        db = ComponentsDatabase(str(self.db_path))

        mfr_id = db.manufacturerId("Samsung")
        assert mfr_id is not None
        assert mfr_id > 0

        db.close()

    def test_manufacturer_id_duplicate_returns_same(self):
        """ComponentsDatabase manufacturerId returns same ID for duplicate."""
        db = ComponentsDatabase(str(self.db_path))

        mfr_id_1 = db.manufacturerId("Samsung")
        mfr_id_2 = db.manufacturerId("Samsung")

        assert mfr_id_1 == mfr_id_2
        db.close()

    def test_manufacturer_id_caching(self):
        """ComponentsDatabase caches manufacturer IDs."""
        db = ComponentsDatabase(str(self.db_path))

        db.manufacturerId("Intel")
        assert "Intel" in db.manufacturer_cache

        cached_id = db.manufacturer_cache["Intel"]
        new_id = db.manufacturerId("Intel")

        assert cached_id == new_id
        db.close()

    def test_manufacturer_id_different_manufacturers(self):
        """ComponentsDatabase assigns different IDs to different manufacturers."""
        db = ComponentsDatabase(str(self.db_path))

        samsung_id = db.manufacturerId("Samsung")
        intel_id = db.manufacturerId("Intel")

        assert samsung_id != intel_id
        db.close()

    def test_category_id_new_category(self):
        """ComponentsDatabase categoryId inserts new category."""
        db = ComponentsDatabase(str(self.db_path))

        cat_id = db.categoryId("Resistors", "Fixed Resistors")
        assert cat_id is not None
        assert cat_id > 0

        db.close()

    def test_category_id_duplicate_returns_same(self):
        """ComponentsDatabase categoryId returns same ID for duplicate."""
        db = ComponentsDatabase(str(self.db_path))

        cat_id_1 = db.categoryId("Resistors", "Fixed Resistors")
        cat_id_2 = db.categoryId("Resistors", "Fixed Resistors")

        assert cat_id_1 == cat_id_2
        db.close()

    def test_category_id_caching(self):
        """ComponentsDatabase caches category IDs."""
        db = ComponentsDatabase(str(self.db_path))

        db.categoryId("Capacitors", "Ceramic")
        key = ("Capacitors", "Ceramic")
        assert key in db.category_cache

        cached_id = db.category_cache[key]
        new_id = db.categoryId("Capacitors", "Ceramic")

        assert cached_id == new_id
        db.close()

    def test_category_id_different_subcategories(self):
        """ComponentsDatabase assigns different IDs to different subcategories."""
        db = ComponentsDatabase(str(self.db_path))

        ceramic_id = db.categoryId("Capacitors", "Ceramic")
        film_id = db.categoryId("Capacitors", "Film")

        assert ceramic_id != film_id
        db.close()

    def test_count_components_empty(self):
        """ComponentsDatabase count_components returns 0 for empty database."""
        db = ComponentsDatabase(str(self.db_path))
        count = db.count_components()
        db.close()

        assert count == 0

    def test_count_components_with_data(self):
        """ComponentsDatabase count_components returns correct count."""
        db = ComponentsDatabase(str(self.db_path))

        # Add some components
        for i in range(5):
            _insert_component(
                db,
                100000 + i,
                mfr=f"MFR{i}",
                description=f"Component {i}",
            )
        db.conn.commit()

        count = db.count_components()
        db.close()

        assert count == 5

    def test_count_components_with_where_clause(self):
        """ComponentsDatabase count_components filters with where_clause."""
        db = ComponentsDatabase(str(self.db_path))

        # Add components with different stock levels
        for i in range(5):
            stock = 100 if i < 3 else 0
            _insert_component(
                db,
                100000 + i,
                mfr=f"MFR{i}",
                description=f"Component {i}",
                stock=stock,
            )
        db.conn.commit()

        total = db.count_components()
        in_stock = db.count_components("stock > 0")
        db.close()

        assert total == 5
        assert in_stock == 3

    def test_fetch_components_empty(self):
        """ComponentsDatabase fetch_components returns empty list for empty database."""
        db = ComponentsDatabase(str(self.db_path))

        batches = list(db.fetch_components())
        db.close()

        assert len(batches) == 0

    def test_fetch_components_single_batch(self):
        """ComponentsDatabase fetch_components yields components in batches."""
        db = ComponentsDatabase(str(self.db_path))

        # Add 5 components
        for i in range(5):
            _insert_component(
                db,
                100000 + i,
                mfr=f"MFR{i}",
                description=f"Component {i}",
            )
        db.conn.commit()

        batches = list(db.fetch_components(batch_size=10))
        db.close()

        assert len(batches) == 1
        assert len(batches[0]) == 5

    def test_fetch_components_multiple_batches(self):
        """ComponentsDatabase fetch_components splits into multiple batches."""
        db = ComponentsDatabase(str(self.db_path))

        # Add 25 components
        for i in range(25):
            _insert_component(
                db,
                100000 + i,
                mfr=f"MFR{i}",
                description=f"Component {i}",
            )
        db.conn.commit()

        batches = list(db.fetch_components(batch_size=10))
        db.close()

        assert len(batches) == 3
        assert len(batches[0]) == 10
        assert len(batches[1]) == 10
        assert len(batches[2]) == 5

    def test_fetch_components_with_where_clause(self):
        """ComponentsDatabase fetch_components filters with where_clause."""
        db = ComponentsDatabase(str(self.db_path))

        # Add components with different stock levels
        for i in range(5):
            stock = 100 if i % 2 == 0 else 0
            _insert_component(
                db,
                100000 + i,
                mfr=f"MFR{i}",
                description=f"Component {i}",
                stock=stock,
            )
        db.conn.commit()

        batches = list(db.fetch_components("stock > 0"))
        db.close()

        assert len(batches) == 1
        assert len(batches[0]) == 3

    def test_get_manufacturers_empty(self):
        """ComponentsDatabase get_manufacturers returns empty dict for empty database."""
        db = ComponentsDatabase(str(self.db_path))
        manufacturers = db.get_manufacturers()
        db.close()

        assert len(manufacturers) == 0

    def test_get_manufacturers_with_data(self):
        """ComponentsDatabase get_manufacturers returns all manufacturers."""
        db = ComponentsDatabase(str(self.db_path))

        mfr_samsung = db.manufacturerId("Samsung")
        mfr_intel = db.manufacturerId("Intel")

        manufacturers = db.get_manufacturers()
        db.close()

        assert len(manufacturers) == 2
        assert mfr_samsung is not None
        assert mfr_intel is not None
        assert manufacturers[mfr_samsung] == "Samsung"
        assert manufacturers[mfr_intel] == "Intel"

    def test_get_categories_empty(self):
        """ComponentsDatabase get_categories returns empty dict for empty database."""
        db = ComponentsDatabase(str(self.db_path))
        categories = db.get_categories()
        db.close()

        assert len(categories) == 0

    def test_get_categories_with_data(self):
        """ComponentsDatabase get_categories returns all categories."""
        db = ComponentsDatabase(str(self.db_path))

        cat_resistor = db.categoryId("Resistors", "Fixed")
        cat_capacitor = db.categoryId("Capacitors", "Ceramic")

        categories = db.get_categories()
        db.close()

        assert len(categories) == 2
        assert cat_resistor is not None
        assert cat_capacitor is not None
        assert categories[cat_resistor] == ("Resistors", "Fixed")
        assert categories[cat_capacitor] == ("Capacitors", "Ceramic")

    def test_cols_static_method(self):
        """ComponentsDatabase.cols returns expected column list."""
        cols = ComponentsDatabase.cols()

        assert isinstance(cols, list)
        assert "lcsc" in cols
        assert "category_id" in cols
        assert "manufacturer_id" in cols
        assert "mfr" in cols
        assert "package" in cols
        assert "basic" in cols
        assert "preferred" in cols
        assert "description" in cols
        assert "datasheet" in cols
        assert "stock" in cols
        assert "price" in cols
        assert "extra" in cols
        assert "joints" in cols
        assert "last_update" in cols

    def test_fix_description_method(self):
        """ComponentsDatabase fix_description updates empty descriptions."""
        db = ComponentsDatabase(str(self.db_path))

        # Insert component with empty description but extra JSON with description
        extra = json.dumps({"description": "Fixed Description"})
        _insert_component(
            db,
            100000,
            description="",
            extra=extra,
        )
        db.conn.commit()

        db.fix_description()

        cursor = db.conn.cursor()
        cursor.execute("SELECT description FROM components WHERE lcsc = ?", (100000,))
        result = cursor.fetchone()
        db.close()

        assert result[0] == "Fixed Description"

    def test_cleanup_stock_old_components(self):
        """ComponentsDatabase cleanup_stock sets old components to zero stock."""
        db = ComponentsDatabase(str(self.db_path))

        now = int(time.time())
        eight_days_ago = now - (8 * 24 * 60 * 60)

        # Insert old component with stock
        _insert_component(
            db,
            100000,
            stock=100,
            last_update=eight_days_ago,
        )
        db.conn.commit()

        db.cleanup_stock()

        cursor = db.conn.cursor()
        cursor.execute("SELECT stock FROM components WHERE lcsc = ?", (100000,))
        result = cursor.fetchone()
        db.close()

        assert result[0] == 0

    def test_cleanup_stock_recent_components(self):
        """ComponentsDatabase cleanup_stock preserves recent component stock."""
        db = ComponentsDatabase(str(self.db_path))

        now = int(time.time())

        # Insert recent component with stock
        _insert_component(
            db,
            100000,
            stock=100,
            last_update=now,
        )
        db.conn.commit()

        db.cleanup_stock()

        cursor = db.conn.cursor()
        cursor.execute("SELECT stock FROM components WHERE lcsc = ?", (100000,))
        result = cursor.fetchone()
        db.close()

        assert result[0] == 100

    def test_truncate_old_clears_old_out_of_stock(self):
        """ComponentsDatabase truncate_old clears price/extra for old out-of-stock."""
        db = ComponentsDatabase(str(self.db_path))

        now = int(time.time())
        over_year_ago = now - (400 * 24 * 60 * 60)

        # Insert old out-of-stock component
        db.conn.execute(
            """INSERT INTO components
            (lcsc, category_id, mfr, package, joints, manufacturer_id,
             basic, description, datasheet, stock, price, last_update, last_on_stock)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                100000,
                1,
                "MFR",
                "0805",
                2,
                1,
                1,
                "Component",
                "http://example.com",
                0,
                "[1.00, 2.00]",
                now,
                over_year_ago,
            ),
        )
        db.conn.commit()

        db.truncate_old()

        cursor = db.conn.cursor()
        cursor.execute("SELECT price, extra FROM components WHERE lcsc = ?", (100000,))
        result = cursor.fetchone()
        db.close()

        assert result[0] == "[]"
        assert result[1] == "{}"

    def test_truncate_old_preserves_in_stock(self):
        """ComponentsDatabase truncate_old preserves in-stock components."""
        db = ComponentsDatabase(str(self.db_path))

        now = int(time.time())
        over_year_ago = now - (400 * 24 * 60 * 60)

        # Insert old but in-stock component
        db.conn.execute(
            """INSERT INTO components
            (lcsc, category_id, mfr, package, joints, manufacturer_id,
             basic, description, datasheet, stock, price, last_update, last_on_stock)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                100000,
                1,
                "MFR",
                "0805",
                2,
                1,
                1,
                "Component",
                "http://example.com",
                100,
                "[1.00, 2.00]",
                now,
                over_year_ago,
            ),
        )
        db.conn.commit()

        db.truncate_old()

        cursor = db.conn.cursor()
        cursor.execute("SELECT price, extra FROM components WHERE lcsc = ?", (100000,))
        result = cursor.fetchone()
        db.close()

        # Should be preserved because stock > 0
        assert result[0] == "[1.00, 2.00]"


# ============================================================================
# Constants Tests
# ============================================================================


class TestCreateStatements:
    """Tests for _CREATE_STATEMENTS constants."""

    def test_create_statements_exists(self):
        """_CREATE_STATEMENTS constant exists."""
        assert _CREATE_STATEMENTS is not None
        assert isinstance(_CREATE_STATEMENTS, list)

    def test_create_statements_not_empty(self):
        """_CREATE_STATEMENTS has statements."""
        assert len(_CREATE_STATEMENTS) > 0

    def test_create_statements_are_strings(self):
        """All _CREATE_STATEMENTS are SQL strings."""
        for stmt in _CREATE_STATEMENTS:
            assert isinstance(stmt, str)
            assert "CREATE" in stmt.upper()

    def test_components_table_statement(self):
        """Components table statement exists."""
        assert any("components" in stmt.lower() for stmt in _CREATE_STATEMENTS)

    def test_manufacturers_table_statement(self):
        """Manufacturers table statement exists."""
        assert any("manufacturers" in stmt.lower() for stmt in _CREATE_STATEMENTS)

    def test_categories_table_statement(self):
        """Categories table statement exists."""
        assert any("categories" in stmt.lower() for stmt in _CREATE_STATEMENTS)
