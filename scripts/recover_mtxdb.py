#!/usr/bin/env python3
"""Recover an mtxdb root whose open is refused with
"no readable version baseline covering the history the shared WAL reclaimed".

It COPIES the broken root and writes a conservative `index.versions` into each
pool of the copy. The input is never modified.

Why this is safe: a collection's logical version is bumped by a mutation, and
every mutation consumes at least one WAL LSN, so a version never exceeds the LSN
it was written at. Setting every version to the reclaimed LSN therefore
OVERestimates (it can only cause a spurious optimistic-concurrency conflict,
never a false success). Versions for groups still in the WAL are restored from
the WAL on open and merged with `max`.

It is a deliberate override of mtxdb's baseline guard. Run it on a copy, verify
with verify_mtxdb.py, and only then use the result.

Usage:
  recover_mtxdb.py --broken ROOT --out NEWROOT [--also-ids ROOT ...] \
      [--mtxdb /path/to/mtxdb] [--state-lsn 158740] [--events-lsn 158727]

Take the LSNs from the refusal message ("...reclaimed ... through LSN N") for the
pool it names; the state pool is checked first, the events pool second.
"""

import argparse
import shutil
import struct
import subprocess
import sys
import zlib
from pathlib import Path

MAGIC = b"MTXVERS2"
POOLS = (("state", "mtpl-state"), ("events", "mtpl-event"))


def collection_ids(mtxdb: str, root: Path, pool: str) -> set[bytes]:
    """Collection ids of a pool, from the read-only `mtxdb collections` report."""
    out = subprocess.run(
        [mtxdb, "-d", str(root), "-t", pool, "collections", "-l", "0"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return {
        bytes.fromhex(line.split()[0][2:])
        for line in out.splitlines()
        if line.startswith("0x")
    }


def read_versions(path: Path) -> set[bytes]:
    """Collection ids already named by an existing index.versions (may be absent)."""
    if not path.exists():
        return set()
    data = path.read_bytes()
    if len(data) < 24 or data[:8] != MAGIC:
        return set()
    count = struct.unpack("<I", data[16:20])[0]
    return {data[20 + 24 * i : 36 + 24 * i] for i in range(count)}


def write_versions(path: Path, ids: set[bytes], lsn: int) -> None:
    body = (
        MAGIC
        + struct.pack("<Q", lsn)
        + struct.pack("<I", len(ids))
        + b"".join(cid + struct.pack("<Q", lsn) for cid in sorted(ids))
    )
    tmp = path.with_suffix(".tmp")
    tmp.write_bytes(body + struct.pack("<I", zlib.crc32(body) & 0xFFFFFFFF))
    tmp.replace(path)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--broken", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument(
        "--also-ids",
        action="append",
        default=[],
        type=Path,
        help="another root whose collection ids to include (e.g. the rebuilt root)",
    )
    ap.add_argument("--mtxdb", default="mtxdb")
    ap.add_argument("--state-lsn", type=int, default=158740)
    ap.add_argument("--events-lsn", type=int, default=158727)
    args = ap.parse_args()

    if args.out.exists():
        sys.exit(f"refusing to overwrite {args.out}")
    shutil.copytree(args.broken, args.out, symlinks=True)
    lsns = {"state": args.state_lsn, "events": args.events_lsn}

    for pool, directory in POOLS:
        pool_dir = args.out / directory
        if not pool_dir.is_dir():
            continue
        path = pool_dir / "index.versions"
        ids = read_versions(path) | collection_ids(args.mtxdb, args.out, pool)
        for other in args.also_ids:
            ids |= collection_ids(args.mtxdb, other, pool)
        write_versions(path, ids, lsns[pool])
        print(f"{pool}: {len(ids)} collections, baseline LSN {lsns[pool]}")
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
