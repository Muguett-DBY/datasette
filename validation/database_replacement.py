"""Exercise normal commits versus file replacement using Datasette's database API.

Linux, disposable databases only. This does not reproduce a dashboard plugin,
Docker Compose stack or the original reporter's unspecified pipeline.
"""

import asyncio
from contextlib import closing
import hashlib
import importlib.metadata
import inspect
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile

from datasette.app import Datasette
from datasette.database import Database

EXPECTED_DATABASE_SHA256 = (
    "fb8dd9ef75f91790f5ca7483b76be02048d9568f763aa9d13dc0f478230b89ac"
)


def create_database(path, value):
    with closing(sqlite3.connect(path)) as conn:
        with conn:
            conn.execute("create table items (value text not null)")
            conn.execute("insert into items values (?)", (value,))


def update_database(path, value):
    with closing(sqlite3.connect(path)) as conn:
        with conn:
            conn.execute("update items set value = ?", (value,))


async def values(db):
    result = await db.execute("select value from items")
    return [row[0] for row in result.rows]


def stop(ds, db):
    # These are disposable read pools. Stop their workers before temp cleanup.
    if ds.executor is not None:
        ds.executor.shutdown(wait=True)
    for conn in db._all_file_connections:
        conn.close()


async def main():
    assert sys.platform == "linux"
    assert importlib.metadata.version("datasette") == "0.65.5"
    module_file = Path(inspect.getfile(Database))
    actual_hash = hashlib.sha256(module_file.read_bytes()).hexdigest()
    assert actual_hash == EXPECTED_DATABASE_SHA256, (
        actual_hash,
        EXPECTED_DATABASE_SHA256,
    )
    report = {
        "python": sys.version.split()[0],
        "platform": sys.platform,
        "datasette": importlib.metadata.version("datasette"),
        "sqlite": sqlite3.sqlite_version,
        "databaseSourceSha256": actual_hash,
    }
    with tempfile.TemporaryDirectory(
        prefix="datasette-replacement-control-"
    ) as directory:
        root = Path(directory)
        path = root / "transaction.db"
        create_database(path, "old")
        ds = Datasette([str(path)], settings={"num_sql_threads": 1})
        db = ds.get_database("transaction")
        assert db.is_mutable
        try:
            old = await values(db)
            inode = path.stat().st_ino
            update_database(path, "committed")
            after = [await values(db) for _ in range(3)]
            assert old == ["old"] and after == [["committed"]] * 3
            assert path.stat().st_ino == inode
            report["inPlaceTransaction"] = {
                "before": old,
                "after": after,
                "sameInode": True,
                "mutable": db.is_mutable,
            }
        finally:
            stop(ds, db)

        path = root / "replaced.db"
        replacement = root / "replacement.db"
        create_database(path, "old")
        create_database(replacement, "replacement")
        ds = Datasette([str(path)], settings={"num_sql_threads": 1})
        db = ds.get_database("replaced")
        fresh_ds = fresh_db = None
        assert db.is_mutable
        try:
            old = await values(db)
            inode = path.stat().st_ino
            os.replace(replacement, path)
            after = [await values(db) for _ in range(3)]
            assert old == ["old"] and after == [["old"]] * 3
            assert path.stat().st_ino != inode
            with closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True)) as conn:
                fresh_sqlite = [
                    row[0] for row in conn.execute("select value from items")
                ]
            fresh_ds = Datasette([str(path)], settings={"num_sql_threads": 1})
            fresh_db = fresh_ds.get_database("replaced")
            fresh_datasette = await values(fresh_db)
            assert fresh_sqlite == fresh_datasette == ["replacement"]
            report["fileReplacement"] = {
                "before": old,
                "existingPoolAfter": after,
                "newSQLiteConnection": fresh_sqlite,
                "newDatasettePool": fresh_datasette,
                "sameInode": False,
                "mutable": db.is_mutable,
            }
        finally:
            if fresh_ds is not None:
                stop(fresh_ds, fresh_db)
            stop(ds, db)
    print("DATASETTE_REPLACEMENT_RESULT=" + json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    asyncio.run(main())
