#!/usr/bin/env python3

"""Use the amazing work of https://github.com/yaqwsx/jlcparts and convert their database into something we can conveniently use for this plugin.

This replaces the old .csv based database creation that JLCPCB no longer supports.
"""

import copy
from datetime import date, datetime
import json
import os
from pathlib import Path
import sqlite3
import sys
import time
from typing import Any
import zipfile
from zipfile import ZipFile

# Add parent directory to path so we can import common module
# TODO(z2amiller):  Use proper packaging
sys.path.insert(0, str(Path(__file__).parent.parent))

import click
import humanize

from common.componentdb import ComponentsDatabase
from common.filemgr import FileManager
from common.jlcapi import CategoryFetch, Component, JlcApi
from common.progress import TqdmNestedProgressBar


class PriceEntry:
    """Price for a quantity range."""

    def __init__(self, min_quantity: int, max_quantity: int | None, price_dollars: str):
        self.min_quantity = min_quantity
        self.max_quantity = max_quantity
        self.price_dollars_str = price_dollars
        self.price_dollars = float(self.price_dollars_str)

    @classmethod
    def Parse(cls, price_entry: dict[str, str]):
        """Parse an individual price entry."""

        price_dollars_str = price_entry["price"]

        min_quantity = int(price_entry["qFrom"])
        max_quantity = (
            int(price_entry["qTo"]) if price_entry.get("qTo") is not None else None
        )

        return cls(min_quantity, max_quantity, price_dollars_str)

    def __repr__(self):
        """Conversion to string function."""
        return f"{self.min_quantity}-{self.max_quantity if self.max_quantity is not None else ''}:{self.price_dollars_str}"

    min_quantity: int
    max_quantity: int | None
    price_dollars_str: str  # to avoid rounding due to float conversion
    price_dollars: float


class Price:
    """Price parsing and management functions."""

    def __init__(self, part_price: list[dict[str, str]]):
        """Format of part_price is determined by json.loads()."""
        self.price_entries = []
        for price in part_price:
            self.price_entries.append(PriceEntry.Parse(price))

    price_entries: list[PriceEntry]

    @staticmethod
    def reduce_precision(entries: list[PriceEntry]) -> list[PriceEntry]:
        """Reduce the precision of price entries to 3 significant digits."""

        """Values after this are not particularly helpful unless many thousands
        of the part is used, and at those quantities of boards and parts
        the contract manufacturer is likely to have special deals."""

        pe = entries
        for i in range(len(pe)):
            pe[i].price_dollars_str = f"{pe[i].price_dollars:.3f}"
            pe[i].price_dollars = round(pe[i].price_dollars, 3)

        return entries

    @staticmethod
    def filter_below_cutoff(
        entries: list[PriceEntry], cutoff_price_dollars: float
    ) -> list[PriceEntry]:
        """Remove PriceEntry values with a price_dollars below cutoff_price_dollars. Keep the first entry if one exists. Assumes order is highest price to lowest price."""

        filtered_entries: list[PriceEntry] = []

        # some components have no price entries
        if len(entries) >= 1:
            # always include the first entry.
            filtered_entries.append(entries[0])
            for entry in entries[1:]:
                # add the entries with a price greater than the cutoff
                if entry.price_dollars >= cutoff_price_dollars:
                    filtered_entries.append(entry)

        if len(filtered_entries) > 0:
            # ensure the last entry in the list has a max_quantity of None
            # as that price continues out indefinitely
            filtered_entries[len(filtered_entries) - 1].max_quantity = None

        return filtered_entries

    @staticmethod
    def filter_duplicate_prices(entries: list[PriceEntry]) -> list[PriceEntry]:
        """Remove entries with duplicate price_dollar_str values, merging quantities so there aren't gaps."""

        # copy.deepcopy() is used to value modifications from altering the original values.
        price_entries_unique: list[PriceEntry] = []
        if len(entries) > 1:
            first = 0
            second = 1
            f: PriceEntry | None = None
            while True:
                if f is None:
                    f = copy.deepcopy(entries[first])

                # stop when the second element is at the end of the list
                if second >= len(entries):
                    break

                # if match, copy over the quantity and advance the second, keep searching for a mismatch
                if f.price_dollars_str == entries[second].price_dollars_str:
                    f.max_quantity = entries[second].max_quantity
                    second += 1
                else:  # if no match, add the first and then start looking at the second
                    price_entries_unique.append(f)
                    first = second
                    second = first + 1
                    f = None

            # always add the final first entry when we run out of elements to process
            price_entries_unique.append(f)
        else:  # only a single entry, nothing to de-duplicate
            price_entries_unique = entries

        return price_entries_unique


class Generate:
    """Base class for database generation."""

    def __init__(
        self,
        output_db: Path,
        chunk_num: Path = Path("chunk_num_fts5.txt"),
        jlcparts_db_name: str = "cache.sqlite3",
        obsolete_parts_threshold_days: int = 0,
        skip_cleanup: bool = False,
    ):
        self.output_db = output_db
        self.jlcparts_db_name = jlcparts_db_name
        self.compressed_output_db = f"{self.output_db}.zip"
        self.chunk_num = chunk_num
        self.skip_cleanup = skip_cleanup
        self.obsolete_parts_threshold_days = obsolete_parts_threshold_days

    def remove_original(self):
        """Remove the original output database."""
        if self.output_db.exists():
            self.output_db.unlink()

    def connect_sqlite(self):
        """Connect to the sqlite databases."""
        # connection to the jlcparts db
        db_uri = f"file:{self.jlcparts_db_name}?mode=rw"
        self.conn_jp = sqlite3.connect(db_uri, uri=True)

        # connection to the plugin db we want to write
        self.conn = sqlite3.connect(self.output_db)

    def meta_data(self):
        """Populate the metadata table."""
        # metadata
        db_size = os.stat(self.output_db).st_size
        self.conn.execute(
            "INSERT INTO meta VALUES(?, ?, ?, ?, ?)",
            [
                "cache.sqlite3",
                db_size,
                self.part_count,
                date.today(),
                datetime.now().isoformat(),
            ],
        )
        self.conn.commit()

    def close_sqlite(self):
        """Close sqlite connections."""
        self.conn_jp.close()
        self.conn.close()

    def split(self):
        """Split the compressed database so we stay below GitHub's 100MB limit.

        Uses FileManager to split the file and create a sentinel file.
        This maintains compatibility with the previous output format.
        """
        file_manager = FileManager(
            file_path=self.output_db,
            chunk_size=80000000,  # 80 MB to stay well below GitHub's 100MB limit
            sentinel_filename=str(self.chunk_num),
        )
        file_manager.split()

    def display_stats(self):
        """Print out some stats."""
        jlcparts_db_size = humanize.naturalsize(os.path.getsize(self.jlcparts_db_name))
        print(f"jlcparts database ({self.jlcparts_db_name}): {jlcparts_db_size}")
        print(f"part count: {humanize.intcomma(self.part_count)}")
        print(
            f"output db: {humanize.naturalsize(os.path.getsize(self.output_db.name))}"
        )

    def cleanup(self):
        """Remove the compressed zip file und output db after splitting."""

        print(f"Deleting {self.output_db}")
        os.unlink(self.output_db)

    def component_where_clause(self) -> str:
        """Return the WHERE clause for filtering components."""
        if self.obsolete_parts_threshold_days > 0:
            # filter out parts that have been obsolete for longer than the threshold
            filter_seconds = (
                int(time.time()) - self.obsolete_parts_threshold_days * 24 * 60 * 60
            )
            return f" WHERE NOT (stock = 0 AND last_on_stock < {filter_seconds})"
        else:
            return ""

    def create_tables(self):
        """Create tables."""

        # Columns are unindexed to save space in the FTS5 index (and overall database)
        #
        # Solder Joint is unindexed as it contains a numerical count that isn't particular helpful for token searching
        # Price is unindexed as it isn't helpful for token searching
        # Stock is unindexed as it isn't helpful for token searching
        self.conn.execute(
            """
            CREATE virtual TABLE IF NOT EXISTS parts using fts5 (
                'LCSC Part',
                'First Category',
                'Second Category',
                'MFR.Part',
                'Package',
                'Solder Joint' unindexed,
                'Manufacturer',
                'Library Type',
                'Description',
                'Datasheet' unindexed,
                'Price' unindexed,
                'Stock' unindexed
            , tokenize="trigram")
            """
        )

        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS mapping (
                'footprint',
                'value',
                'LCSC'
            )
            """
        )

        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS meta (
                'filename',
                'size',
                'partcount',
                'date',
                'last_update'
            )
            """
        )

        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS categories (
                'First Category',
                'Second Category'
            )
            """
        )

    @staticmethod
    def library_type(row: sqlite3.Row) -> str:
        """Return library type string."""
        if row["basic"]:
            return "Basic"
        if row["preferred"]:
            return "Preferred"
        return "Extended"

    def load_tables(self):
        """Load the input data into the output database."""

        # load the tables into memory
        print("Reading manufacturers")
        res = self.conn_jp.execute("SELECT * FROM manufacturers")
        mans = dict(res.fetchall())

        print("Reading categories")
        res = self.conn_jp.execute("SELECT * FROM categories")
        cats = {i: (c, sc) for i, c, sc in res.fetchall()}

        res = self.conn_jp.execute(
            f"select count(*) from components {self.component_where_clause()}"
        )
        results = res.fetchone()
        print(f"{humanize.intcomma(results[0])} parts to import")

        price_entries_total = 0
        price_entries_deleted_total = 0
        price_entries_duplicates_deleted_total = 0

        self.part_count = 0
        print("Reading components")
        self.conn_jp.row_factory = sqlite3.Row
        self.conn.row_factory = sqlite3.Row
        res = self.conn_jp.execute(f"""
            SELECT
                lcsc,
                category_id,
                mfr,
                package,
                joints,
                manufacturer_id,
                basic,
                preferred,
                description,
                datasheet,
                stock,
                price,
                extra
            FROM components {self.component_where_clause()}""")
        while True:
            comps = res.fetchmany(size=100000)

            print(f"Read {humanize.intcomma(len(comps))} parts")

            # if we have no more parts exit out of the loop
            if len(comps) == 0:
                break

            self.part_count += len(comps)

            # now extract the data from the jlcparts db and fill
            # it into the plugin database
            print("Building parts rows to insert")
            rows = []
            for c in comps:
                priceInput = json.loads(c["price"])

                # parse the price field
                price = Price(priceInput)

                price_entries = Price.reduce_precision(price.price_entries)
                price_entries_total += len(price_entries)

                price_str: str = ""

                # filter parts priced below the cutoff value
                price_entries_cutoff = Price.filter_below_cutoff(price_entries, 0.01)
                price_entries_deleted_total += len(price_entries) - len(
                    price_entries_cutoff
                )

                # alias the variable for the next step
                price_entries = price_entries_cutoff

                # remove duplicates
                price_entries_unique = Price.filter_duplicate_prices(price_entries)
                price_entries_duplicates_deleted_total += len(price_entries) - len(
                    price_entries_unique
                )
                price_entries_deleted_total += len(price_entries) - len(
                    price_entries_unique
                )

                # alias over the variable for the next step
                price_entries = price_entries_unique

                # build the output string that is stored into the parts database
                price_str = ",".join(
                    [
                        f"{entry.min_quantity}-{entry.max_quantity if entry.max_quantity is not None else ''}:{entry.price_dollars_str}"
                        for entry in price_entries
                    ]
                )

                # default to 'description', override it with the 'description' property from
                # 'extra' if it exists
                description = c["description"]
                extra = {}
                if c["extra"] is not None:
                    try:
                        extra = json.loads(c["extra"])
                        if "description" in extra:
                            description = extra["description"]
                    except Exception:
                        pass

                # strip ROHS out of descriptions where present
                # and add 'not ROHS' where ROHS is not present
                # as 99% of parts are ROHS at this point
                if " ROHS".lower() not in description.lower():
                    description += " not ROHS"
                else:
                    description = description.replace(" ROHS", "")

                second_category = cats[c["category_id"]][1]

                # strip the 'Second category' out of the description if it
                # is duplicated there
                description = description.replace(second_category, "")

                package = c["package"]

                # remove 'Package' from the description if it is duplicated there
                description = description.replace(package, "")

                # replace double spaces with single spaces in description
                description.replace("  ", " ")

                # remove trailing spaces from description
                description = description.strip()

                libType = self.library_type(c)

                row = {
                    "LCSC Part": f"C{c['lcsc']}",
                    "First Category": cats[c["category_id"]][0],
                    "Second Category": cats[c["category_id"]][1],
                    "MFR.Part": c["mfr"],
                    "Package": c["package"],
                    "Solder Joint": int(c["joints"]),
                    "Manufacturer": mans[c["manufacturer_id"]],
                    "Library Type": libType,
                    "Description": description,
                    "Datasheet": c["datasheet"],
                    "Price": price_str,
                    "Stock": str(c["stock"]),
                }
                rows.append(row)

            print("Inserting into parts table")
            # The column names have spaces, so map them to placeholders without spaces
            data = rows[0]
            columns = ", ".join([f'"{k}"' for k in data])
            placeholders = ", ".join(
                [f":{k.replace(' ', '_').replace('.', '_')}" for k in data]
            )
            newrows = [
                {k.replace(" ", "_").replace(".", "_"): v for k, v in row.items()}
                for row in rows
            ]
            self.conn.executemany(
                f"INSERT INTO parts ({columns}) VALUES ({placeholders})", newrows
            )
            self.conn.commit()

        print(
            f"Price value filtering trimmed {price_entries_deleted_total} (including {price_entries_duplicates_deleted_total} duplicates) out of {price_entries_total} entries {(price_entries_deleted_total / price_entries_total) * 100 if price_entries_total != 0 else 0:.2f}%"
        )
        print("Done importing parts")

    def populate_categories(self):
        """Populate the categories table."""
        self.conn.execute(
            'INSERT INTO categories SELECT DISTINCT "First Category", "Second Category" FROM parts ORDER BY UPPER("First Category"), UPPER("Second Category")'
        )

    def optimize(self):
        """FTS5 optimize to minimize query times."""
        print("Optimizing fts5 parts table")
        self.conn.execute("insert into parts(parts) values('optimize')")
        print("Done optimizing fts5 parts table")

    def build(self):
        """Run all of the steps to generate the database files for upload."""
        self.remove_original()
        self.connect_sqlite()
        self.create_tables()
        self.load_tables()
        self.populate_categories()
        self.optimize()
        self.meta_data()
        self.close_sqlite()
        self.split()
        self.display_stats()
        if self.skip_cleanup:
            print("Skipping cleanup")
        else:
            self.cleanup()


class DownloadProgress:
    """Display the download status during the download process."""

    def __init__(self):
        self.last_download_progress_print_time = 0

    def progress_hook(self, count, block_size, total_size):
        """Pass to reporthook."""
        downloaded = count * block_size

        # print at most twice a second
        max_time_between_prints_seconds = 0.5

        now = time.monotonic()
        if (
            now - self.last_download_progress_print_time
            >= max_time_between_prints_seconds
            or count * block_size >= total_size
        ):
            percent = int(downloaded * 100 / total_size) if total_size > 0 else 0

            sys.stdout.write(
                f"\rDownloading: {percent}% ({downloaded}/{total_size} bytes)"
            )
            sys.stdout.flush()
            self.last_download_progress_print_time = now

        if downloaded >= total_size:
            print()  # Finish line


def test_price_precision_reduce():
    """Price precision reduction works as expected."""

    # build high precision price entries
    prices: list[PriceEntry] = []
    initial_price = "0.123456789"
    prices.append(PriceEntry(1, 100, initial_price))

    # run through precision change
    lower_precision_prices = Price.reduce_precision(prices)

    # confirm 3 digits of precision remain
    expected_price_str = "0.123"
    expected_price_val = 0.123

    print(f"{lower_precision_prices[0]}")

    assert lower_precision_prices[0].price_dollars_str == expected_price_str
    assert lower_precision_prices[0].price_dollars == expected_price_val


def test_price_filter_below_cutoff():
    """Price filter below cutoff works as expected."""

    # build price list with some prices lower than the cutoff
    prices: list[PriceEntry] = []
    prices.append(PriceEntry(1, 100, "0.4"))
    prices.append(PriceEntry(101, 200, "0.3"))
    prices.append(PriceEntry(201, 300, "0.2"))
    prices.append(PriceEntry(301, 400, "0.1"))

    # run through cutoff deletion filter
    filtered_prices = Price.filter_below_cutoff(prices, 0.3)

    # confirm prices lower than cutoff were deleted
    assert len(filtered_prices) == 2
    assert filtered_prices[0].price_dollars == 0.4
    assert filtered_prices[1].price_dollars == 0.3


def test_price_duplicate_price_filter():
    """Price duplicates are removed."""
    # build price list with duplicates
    prices: list[PriceEntry] = []
    prices.append(PriceEntry(1, 100, "0.4"))
    prices.append(PriceEntry(101, 200, "0.3"))
    prices.append(PriceEntry(201, 300, "0.2"))
    prices.append(PriceEntry(301, 400, "0.1"))
    prices.append(PriceEntry(401, 500, "0.1"))
    prices.append(PriceEntry(501, 600, "0.1"))
    prices.append(PriceEntry(601, None, "0.1"))

    # run duplicate filter
    unique = Price.filter_duplicate_prices(prices)

    # confirm duplicates were removed
    assert len(unique) == 4
    assert unique[len(unique) - 1].price_dollars_str == "0.1"

    # last value max_quantity is None
    assert unique[len(unique) - 1].max_quantity is None


def update_parts_db_from_api() -> None:
    """Update the component cache database."""
    db = ComponentsDatabase("cache_archive/cache.sqlite3")
    print("Fetching categories...")
    initial_categories = JlcApi.fetchCategories(instockOnly=True)
    categories = JlcApi.collapseCategories(initial_categories, limit=50000)
    print(f"Found {len(initial_categories)} categories, collaped to {len(categories)}.")

    progress = TqdmNestedProgressBar()

    with progress.outer(len(categories), "Fetching categories") as outer_pbar:
        for category in categories:
            fetcher = CategoryFetch(category)

            with progress.inner(category.count, f"{category}") as inner_pbar:
                for components in fetcher.fetchAll():
                    comp_objs = [Component(comp) for comp in components]
                    db.update_cache(comp_objs)
                    inner_pbar.update(len(components))

            outer_pbar.update()

    db.cleanup_stock()
    db.close()


@click.command()
@click.option(
    "--skip-cleanup",
    is_flag=True,
    show_default=True,
    default=False,
    help="Disable cleanup, intermediate database files will not be deleted",
)
@click.option(
    "--parts-db-base-url",
    default="http://yaqwsx.github.io/jlcparts/data",
    show_default=True,
    help="Base URL to fetch the parts database from",
)
@click.option(
    "--fix-parts-db-descriptions",
    is_flag=True,
    show_default=True,
    default=False,
    help="Fix descriptions in the parts db by pulling from the 'extra' field",
)
@click.option(
    "--update-parts-db",
    is_flag=True,
    show_default=True,
    default=False,
    help="Update the local parts db using LCSC API data",
)
@click.option(
    "--clean-parts-db",
    is_flag=True,
    show_default=True,
    default=False,
    help="Clean the local parts db by removing old and out-of-stock parts",
)
@click.option(
    "--archive-parts-db",
    is_flag=True,
    show_default=True,
    default=False,
    help="Archive the parts db after updating from the API",
)
@click.option(
    "--skip-generate",
    is_flag=True,
    show_default=True,
    default=False,
    help="Skip the DB generation phase",
)
@click.option(
    "--obsolete-parts-threshold-days",
    show_default=True,
    default=0,
    type=int,
    help="""
        Setting this to > 0 will filter out parts that have a stock level of zero
        in the source parts database for at least this many days.
    """,
)
def main(
    skip_cleanup: bool,
    parts_db_base_url: str,
    fix_parts_db_descriptions: bool,
    update_parts_db: bool,
    clean_parts_db: bool,
    archive_parts_db: bool,
    skip_generate: bool,
    obsolete_parts_threshold_days: int,
):
    """Perform the database steps."""

    components_db = "cache_archive/cache.sqlite3"

    output_directory = "db_working"
    if not os.path.exists(output_directory):
        os.mkdir(output_directory)

    if fix_parts_db_descriptions:
        print("Fixing parts database descriptions")
        db = ComponentsDatabase(components_db)
        db.fix_description()
        db.close()

    if update_parts_db:
        update_parts_db_from_api()

    if clean_parts_db:
        print("Cleaning parts database")
        db = ComponentsDatabase(components_db)
        db.truncate_old()
        db.close()

    os.chdir(output_directory)
    if not skip_generate:
        # sqlite database
        start = datetime.now()
        output_name = "parts-fts5.db"
        partsdb = Path(output_name)

        print(f"Generating {output_name} in {output_directory} directory")
        generator = Generate(
            output_db=partsdb,
            skip_cleanup=skip_cleanup,
            obsolete_parts_threshold_days=obsolete_parts_threshold_days,
            jlcparts_db_name=f"../{components_db}",  # TODO(z2amiller): Fix this hack
        )
        generator.build()

        end = datetime.now()
        deltatime = end - start
        print(
            f"Elapsed time: {humanize.precisedelta(deltatime, minimum_unit='seconds')}"
        )

    os.chdir("..")
    if archive_parts_db:
        fm = FileManager(
            file_path=Path(components_db),
            chunk_size=50 * 1024 * 1024,  # 50 MB
            sentinel_filename="cache_chunk_num.txt",
        )
        fm.split(output_dir=Path("cached_archive"), delete_original=skip_cleanup)


if __name__ == "__main__":
    main()
