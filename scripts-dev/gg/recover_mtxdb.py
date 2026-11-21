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

`covered_lsn` is different: it must NOT be raised past what the pool really
covers, or replay skips retained WAL groups. It is set to the per-pool mark the
source root's WAL header records (the number in the
"...reclaimed ... through LSN N" refusal), cross-checked against
`mtxdb meta wal`. An override is accepted only inside
[mark, base_lsn - 1].

Collections are enumerated from the physical pack frames (`mtxdb packs dump`),
not from the index, which is the unreliable part.

It also refuses if a newer ACTIVE root could hold data the broken root lacks:
any difference in the latest value of any record (new, missing or changed), or
any group still in the active WAL. The script has no merge. Pass --active ROOT
(a frozen copy, not the live directory) to enable the check.

It is a deliberate override of mtxdb's baseline guard. Run it on a copy, verify
with verify_mtxdb.py (and boot the result), and only then use it.

Usage:
  recover_mtxdb.py --broken ROOT --out NEWROOT [--active ROOT] \
      [--also-ids ROOT ...] [--mtxdb /path/to/mtxdb] \
      [--state-lsn N] [--events-lsn N]
"""

import argparse
import hashlib
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


def wal_header(mtxdb: str, root: Path) -> tuple[int, dict[str, int]]:
    """`(base_lsn, per-pool "history reclaimed through LSN" marks)` from the WAL.

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
    return base_lsn, {names[i]: marks[i] for i in range(min(count, len(names)))}


def wal_group_count(mtxdb: str, root: Path) -> int:
    """How many groups the root's shared WAL still holds (`mtxdb meta wal`)."""
    report = subprocess.run(
        [mtxdb, "-d", str(root), "meta", "wal"], capture_output=True, text=True
    ).stdout
    match = re.search(r"(\d+) group\(s\)", report)
    if not match:
        sys.exit(f"{root}: cannot read the WAL group count; refusing to guess")
    return int(match.group(1))


def record_latest(
    mtxdb: str, root: Path, pool: str, directory: str
) -> dict[tuple[str, str], str]:
    """`(collection, node) -> digest of its latest frame`, from the pack frames.

    Reads the packs themselves, not the index. Packs are ordered by mtime then
    name and frames by offset, so a later frame for the same key wins, as it
    does when the pool is opened. The digest covers the payload and metadata, so
    a rewritten value differs from the original even though the key is the same.
    """
    latest: dict[tuple[str, str], str] = {}
    packs = sorted(
        (root / directory).glob("pack_*.pack"),
        key=lambda path: (path.stat().st_mtime, path.name),
    )
    for pack in packs:
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
            digest = hashlib.sha256(
                (
                    rec.get("payload_base64", "")
                    # `{}` and `null` both mean "no metadata" (a repack rewrites one
                    # as the other); anything else is part of the value.
                    + json.dumps(rec.get("metadata") or {}, sort_keys=True)
                ).encode()
            ).hexdigest()
            latest[(rec["collection_id"], rec["node_id"])] = digest
    if packs and not latest:
        sys.exit(
            f"{root}/{directory}: has pack files but the frame scan found no "
            "records; refusing to build a baseline from nothing"
        )
    return latest


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
    ap.add_argument("--state-lsn", type=int, help="override (bounded by the WAL)")
    ap.add_argument("--events-lsn", type=int, help="override (bounded by the WAL)")
    ap.add_argument(
        "--active",
        type=Path,
        help="a frozen copy of the newer root in service; refuse if it differs",
    )
    args = ap.parse_args()

    if args.out.exists():
        sys.exit(f"refusing to overwrite {args.out}")

    base_lsn, marks = wal_header(args.mtxdb, args.broken)
    lsns = {}
    for pool, override in (("state", args.state_lsn), ("events", args.events_lsn)):
        mark = marks.get(pool, 0)
        lsn = override if override is not None else mark
        if not mark <= lsn <= max(base_lsn - 1, mark):
            sys.exit(
                f"{pool}: LSN {lsn} is outside [{mark}, {base_lsn - 1}]. Below the "
                "mark the guard still refuses; above base_lsn - 1 replay would "
                "skip retained WAL groups."
            )
        lsns[pool] = lsn
    print(f"WAL base_lsn {base_lsn}; marks {marks}; using {lsns}")

    frames = {
        pool: record_latest(args.mtxdb, args.broken, pool, directory)
        for pool, directory in POOLS
        if (args.broken / directory).is_dir()
    }

    if args.active:
        groups = wal_group_count(args.mtxdb, args.active)
        if groups:
            sys.exit(
                f"the active root's WAL still holds {groups} group(s) that a pack "
                "scan cannot see; this script does not merge them. Refusing."
            )
        problems = 0
        for pool, directory in POOLS:
            if not (
                (args.broken / directory).is_dir() or (args.active / directory).is_dir()
            ):
                continue
            ours = frames.get(pool, {})
            theirs = record_latest(args.mtxdb, args.active, pool, directory)
            new = len(theirs.keys() - ours.keys())
            gone = len(ours.keys() - theirs.keys())
            changed = sum(
                1 for k in ours.keys() & theirs.keys() if ours[k] != theirs[k]
            )
            print(
                f"{pool}: active vs broken -> new {new}, missing {gone}, "
                f"changed value {changed}"
            )
            problems += new + gone + changed
        if problems:
            sys.exit(
                "the active root differs from the broken one; this script does not "
                "merge. Refusing."
            )

    shutil.copytree(args.broken, args.out, symlinks=True)

    for pool, directory in POOLS:
        pool_dir = args.out / directory
        if not pool_dir.is_dir():
            continue
        path = pool_dir / "index.versions"
        # Enumerate from the physical pack frames; the index-based report is
        # only added to, never relied on: a collection missing from the baseline
        # would restart at version 0.
        ids = read_versions(path) | collection_ids(args.mtxdb, args.out, pool)
        ids |= {
            bytes.fromhex(collection[2:]) for collection, _node in frames.get(pool, {})
        }
        for other in args.also_ids:
            ids |= collection_ids(args.mtxdb, other, pool)
        write_versions(path, ids, lsns[pool])
        print(f"{pool}: {len(ids)} collections, baseline LSN {lsns[pool]}")
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
