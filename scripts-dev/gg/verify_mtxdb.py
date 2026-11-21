#!/usr/bin/env python3
"""Verify an mtxdb root the way Synapse uses it: open it WRITABLE and read event
bodies back. (A read-only open can look fine while a writable open returns
nothing -- that is exactly how the repacked root failed.)

Opening writable modifies the root (WAL, read_committed.gen, ...), so this
script opens a THROWAWAY COPY and deletes it afterwards.

Needs the sithnapse python (the synapse_rust extension), e.g.:
  cd /opt/sithnapse && SYNAPSE_MTXDB_WAL=1 bin/python -I verify_mtxdb.py ...
  or in the repo: uv run --no-sync python -I verify_mtxdb.py ...

Usage:
  verify_mtxdb.py ROOT IDS_FILE [--namespace sith.nutra.tk] [--show-missing N]

IDS_FILE: one event id per line, e.g.
  psql sithnapse -Atc "select event_id from events" > ids.txt
"""

import argparse
import shutil
import sys
import tempfile
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("root", type=Path)
    ap.add_argument("ids", type=Path)
    ap.add_argument("--namespace", default="sith.nutra.tk")
    ap.add_argument("--show-missing", type=int, default=5)
    args = ap.parse_args()

    ids = [line.strip() for line in args.ids.read_text().splitlines() if line.strip()]
    tmp = Path(tempfile.mkdtemp(prefix="mtxdb-verify-", dir=args.root.parent))
    copy = tmp / "root"
    try:
        shutil.copytree(args.root, copy, symlinks=False)
        from synapse.storage.databases.embedded_engine import get_embedded_engine

        get_embedded_engine("mtxdb").open_client(str(copy))
        print("writable open: OK")

        from synapse.synapse_rust.mtxdb_engine import event_json_get

        found_count = 0
        missing_count = 0
        missing_samples: list[str] = []
        batch_size = 1000
        for offset in range(0, len(ids), batch_size):
            batch = ids[offset : offset + batch_size]
            found = event_json_get(args.namespace, batch)
            for eid, record in found:
                if record is None:
                    missing_count += 1
                    if len(missing_samples) < args.show_missing:
                        missing_samples.append(eid)
                else:
                    found_count += 1
        print(f"event bodies found: {found_count} / {len(ids)}")
        for eid in missing_samples:
            print("  missing:", eid)
        return 0 if not missing_count else 2
    except Exception as exc:  # noqa: BLE001
        print("writable open FAILED:", str(exc)[:500])
        return 1
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
