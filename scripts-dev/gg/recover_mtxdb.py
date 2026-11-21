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

The per-pool LSNs are read from the source root's own WAL header, which records
for each pool the newest LSN of history a reclaim removed (the number in the
"...reclaimed ... through LSN N" refusal), and are cross-checked against
`mtxdb meta wal`. They can be overridden, but should not need to be.

It also refuses if a newer ACTIVE root holds records the broken root lacks
(for example events persisted after a rebuilt root was started), because this
script does not merge them. Pass --active ROOT to enable that check.

Usage:
  recover_mtxdb.py --broken ROOT --out NEWROOT [--active ROOT] \
      [--also-ids ROOT ...] [--mtxdb /path/to/mtxdb] \
      [--state-lsn N] [--events-lsn N]
"""

import argparse
import json
import re
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


def wal_reclaim_marks(mtxdb: str, root: Path) -> dict[str, int]:
    """Per-pool "history reclaimed through LSN" marks from the WAL header.

    Layout (little endian): magic `MTXWAL01`, u32 version, u64 (unused here), u64
    base_lsn at 0x14, u32 pool count at 0x1c, then one u64 mark per pool in the
    order state, events, srvinfo. `base_lsn` is cross-checked against
    `mtxdb meta wal`; any mismatch aborts rather than guessing.
    """
    header = (root / "wal.bin").read_bytes()[:0x40]
    if header[:8] != b"MTXWAL01":
        sys.exit(f"{root}/wal.bin: not an mtxdb shared WAL")
    base_lsn = struct.unpack("<Q", header[0x14:0x1C])[0]
    count = struct.unpack("<I", header[0x1C:0x20])[0]
    marks = [
        struct.unpack("<Q", header[0x20 + 8 * i : 0x28 + 8 * i])[0]
        for i in range(count)
    ]
    report = subprocess.run(
        [mtxdb, "-d", str(root), "meta", "wal"], capture_output=True, text=True
    ).stdout
    match = re.search(r"base_lsn=(\d+)", report)
    if not match or int(match.group(1)) != base_lsn:
        sys.exit(
            f"WAL header base_lsn {base_lsn} disagrees with `mtxdb meta wal` "
            f"({match.group(1) if match else 'missing'}); refusing to guess marks"
        )
    names = ("state", "events", "srvinfo")
    return {names[i]: marks[i] for i in range(min(count, len(names)))}


def record_pairs(
    mtxdb: str, root: Path, pool: str, directory: str
) -> set[tuple[str, str]]:
    """Every (collection, node) the pool's packs physically hold."""
    pairs: set[tuple[str, str]] = set()
    for pack in sorted((root / directory).glob("pack_*.pack")):
        pack_id = pack.stem.removeprefix("pack_")
        out = subprocess.run(
            [
                mtxdb,
                "-d",
                str(root),
                "-t",
                pool,
                "packs",
                "dump",
                "--pack",
                f"0x{pack_id}",
            ],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        for line in out.splitlines():
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            pairs.add((rec["collection_id"], rec["node_id"]))
    return pairs


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
    ap.add_argument("--state-lsn", type=int, help="override the WAL header mark")
    ap.add_argument("--events-lsn", type=int, help="override the WAL header mark")
    ap.add_argument(
        "--active",
        type=Path,
        help="the newer root in service; refuse if it holds records this one lacks",
    )
    args = ap.parse_args()

    if args.out.exists():
        sys.exit(f"refusing to overwrite {args.out}")

    marks = wal_reclaim_marks(args.mtxdb, args.broken)
    lsns = {
        "state": args.state_lsn or marks.get("state", 0),
        "events": args.events_lsn or marks.get("events", 0),
    }
    print(f"WAL header marks: {marks}; using {lsns}")

    if args.active:
        newer = 0
        for pool, directory in POOLS:
            if not (
                (args.broken / directory).is_dir() or (args.active / directory).is_dir()
            ):
                continue
            extra = record_pairs(
                args.mtxdb, args.active, pool, directory
            ) - record_pairs(args.mtxdb, args.broken, pool, directory)
            print(f"{pool}: {len(extra)} records only in the active root")
            newer += len(extra)
        if newer:
            sys.exit(
                "the active root holds records the broken root lacks; this script "
                "does not merge them. Refusing."
            )

    shutil.copytree(args.broken, args.out, symlinks=True)

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
