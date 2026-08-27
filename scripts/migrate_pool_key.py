"""Safely rename a Flex pool key in the local SQLite state database."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

TABLES = ('attempts','states','quota_resets','channel_overrides','channel_tests','learned_limits')


def main() -> None:
    if len(sys.argv) != 4:
        raise SystemExit('usage: migrate_pool_key.py DB_PATH OLD_POOL NEW_POOL')
    db_path, old, new = sys.argv[1:]
    if old == new:
        raise SystemExit('old and new pool keys are identical')
    db = sqlite3.connect(Path(db_path))
    try:
        for table in TABLES:
            exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
            if exists and db.execute(f'SELECT COUNT(*) FROM {table} WHERE pool=?', (new,)).fetchone()[0]:
                raise SystemExit(f'aborting: target pool already has rows in {table}')
        with db:
            for table in TABLES:
                exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
                if exists:
                    changed = db.execute(f'UPDATE {table} SET pool=? WHERE pool=?', (new, old)).rowcount
                    print(f'{table}: {changed}')
    finally:
        db.close()


if __name__ == '__main__':
    main()
