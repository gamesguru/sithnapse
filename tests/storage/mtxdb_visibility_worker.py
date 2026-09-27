#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#
"""Line-driven mtxdb process for `test_mtxdb_journal_visibility.py`.

    python mtxdb_visibility_worker.py {writer|reader} <store dir> [namespace]

mtxdb's Python binding owns process-global pools, so a writer and a read-only
worker cannot share an interpreter: the test drives each in its own process.
Commands arrive on stdin, one per line, and each is answered with one line.

`event_json_put`/`event_json_get` go through the production
`embedded_event_json` helpers (not raw `batch_put`/`batch_get`): the point of
those cases is that the real event-JSON read path -- the one a worker uses --
sees a write the writer journaled but never fsynced.

`txn_begin`, `txn_event_json_put`, `txn_commit` and `txn_abort` drive one open
`MtxdbTransaction`: its writes are invisible to the worker until `txn_commit`.
"""

import json
import logging
import sys

# The worker's stdout is a line protocol whose first line must be exactly
# "READY" (see test_mtxdb_journal_visibility.py's `_Process.__init__`,
# which reads it with stderr merged into stdout). `synapse.metrics` logs a
# non-fatal compatibility warning at import time on an older-but-still-
# supported `prometheus_client` (its `_use_created` hack has no clean
# feature-detection below 0.14.0 -- see
# `_set_prometheus_client_use_created_metrics`) -- harmless for a normal
# process, but fatal here since it can land before "READY" and get read as
# the readiness line. Blanket-suppress logging for the length of these
# imports (nothing worth seeing happens during import) rather than bumping
# Synapse's prometheus-client floor just to dodge one log line, or trying
# to guess every logger name that might fire during import.
logging.disable(logging.CRITICAL)
from synapse.storage.databases.main.embedded_event_json import (  # noqa: E402
    get_event_json_batch,
    put_event_json_batch,
)
from synapse.synapse_rust import mtxdb_engine  # noqa: E402

logging.disable(logging.NOTSET)

# The worker's stdout is a line protocol; `get_event_json_batch` logs a trace
# that must not land between replies.
logging.getLogger("synapse.storage.databases.main.embedded_event_json").setLevel(
    logging.WARNING
)


def _fsyncs() -> int:
    """Total journal/pack fsyncs this process's engine has issued."""
    stats = mtxdb_engine.stats()
    return sum(
        int((stats.get(pool) or {}).get("sync_totals", {}).get("calls", 0))
        for pool in ("state", "event_dag", "auth_chain")
    )


def main() -> None:
    role, path = sys.argv[1], sys.argv[2]
    namespace = sys.argv[3] if len(sys.argv) > 3 else "vis"
    if role == "writer":
        mtxdb_engine.open_client(path)
    else:
        mtxdb_engine.open_client_read_only(path)
    print("READY", flush=True)
    transaction = None

    for line in sys.stdin:
        command, *args = line.split()
        try:
            if command == "put":
                mtxdb_engine.batch_put([(args[0].encode(), args[1].encode())])
                reply = "ok"
            elif command == "sync":
                mtxdb_engine.sync()
                reply = "ok"
            elif command == "get":
                found = dict(mtxdb_engine.batch_get([args[0].encode()]))
                reply = json.dumps(
                    found[args[0].encode()].decode()
                    if args[0].encode() in found
                    else None
                )
            elif command == "event_json_put":
                # event_json_put <room_id> <event_id> <json> (no spaces in json)
                room_id, event_id, body = args
                put_event_json_batch(
                    "mtxdb",
                    namespace,
                    [(event_id, room_id, "{}", body, 1)],
                    sync=False,
                )
                reply = "ok"
            elif command == "event_json_get":
                found_json = get_event_json_batch("mtxdb", namespace, [args[0]])
                reply = json.dumps(
                    found_json[args[0]][1] if args[0] in found_json else None
                )
            elif command == "txn_begin":
                transaction = mtxdb_engine.begin_transaction()
                if transaction is None:
                    raise RuntimeError("no shared WAL: cannot begin a transaction")
                reply = "ok"
            elif command == "txn_event_json_put":
                room_id, event_id, body = args
                put_event_json_batch(
                    "mtxdb",
                    namespace,
                    [(event_id, room_id, "{}", body, 1)],
                    transaction=transaction,
                )
                reply = "ok"
            elif command == "txn_commit":
                assert transaction is not None
                transaction.commit()
                reply = "ok"
            elif command == "txn_abort":
                assert transaction is not None
                transaction.abort()
                reply = "ok"
            elif command == "fsyncs":
                reply = str(_fsyncs())
            elif command == "exit":
                print("bye", flush=True)
                return
            else:
                reply = f"ERR unknown command {command}"
        except Exception as e:
            reply = f"ERR {type(e).__name__}: {e}"
        print(reply, flush=True)


if __name__ == "__main__":
    main()
