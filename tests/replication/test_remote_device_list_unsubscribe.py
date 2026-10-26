#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#
from tests.replication._base import BaseMultiWorkerStreamTestCase

REMOTE_USER = "@elsie:remote"


class RemoteDeviceListUnsubscribeTestCase(BaseMultiWorkerStreamTestCase):
    def test_unsubscribe_reaches_worker_without_resync(self) -> None:
        """Unsubscribing a remote user stops another worker treating their
        device list as cached, without queueing a device list resync.

        Sync unsubscribes remote users it reports as left so that the next key
        query refetches their devices. Workers read the device list extremity
        through a cache, so this checks the invalidation reaches them. Marking
        the user stale as well would force the same miss, but also makes the
        resync retry loop fetch the list from the remote server.
        """
        store = self.hs.get_datastores().main
        self.get_success(
            store.update_remote_device_list_cache(
                REMOTE_USER, [{"device_id": "DEVICE", "keys": {}}], stream_id=1
            )
        )

        worker = self.make_worker_hs("synapse.app.generic_worker")
        worker_store = worker.get_datastores().main
        self.assertEqual(
            self.get_success(
                worker_store.get_users_whose_devices_are_cached([REMOTE_USER])
            ),
            {REMOTE_USER},
        )

        self.get_success(
            store.mark_remote_user_device_list_as_unsubscribed(REMOTE_USER)
        )
        self.replicate()

        self.assertEqual(
            self.get_success(
                worker_store.get_users_whose_devices_are_cached([REMOTE_USER])
            ),
            set(),
        )
        self.assertEqual(
            self.get_success(
                store.get_user_ids_requiring_device_list_resync([REMOTE_USER])
            ),
            set(),
        )
