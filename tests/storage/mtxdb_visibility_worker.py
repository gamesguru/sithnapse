#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#
"""Line-driven mtxdb process for `test_mtxdb_publish_visibility.py`.

    python mtxdb_visibility_worker.py {writer|reader} <store dir>

mtxdb's Python binding owns process-global pools, so a writer and a read-only
worker cannot share an interpreter: the test drives each in its own process.
Commands arrive on stdin, one per line, and each is answered with one line.
"""

import json
import sys

from synapse.synapse_rust import mtxdb_engine


def _fsyncs() -> int:
    """Total journal/pack fsyncs this process's engine has issued."""
    stats = mtxdb_engine.stats()
    return sum(
        int((stats.get(pool) or {}).get("sync_totals", {}).get("calls", 0))
        for pool in ("state", "event_dag", "auth_chain")
    )


def main() -> None:
    role, path = sys.argv[1], sys.argv[2]
    if role == "writer":
        mtxdb_engine.open_client(path)
    else:
        mtxdb_engine.open_client_read_only(path)
    print("READY", flush=True)

    for line in sys.stdin:
        command, *args = line.split()
        try:
            if command == "put":
                mtxdb_engine.batch_put([(args[0].encode(), args[1].encode())])
                reply = "ok"
            elif command == "publish":
                mtxdb_engine.publish_pending()
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
