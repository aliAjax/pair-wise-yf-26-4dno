import base64
import hashlib
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from app import BusinessError, PreservationStore


class PreservationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = PreservationStore(Path(self.tmp.name) / "test.db")
        self.store.seed()
        self.archive = self.store.create_archive("owner", "城市测绘档案", (date.today() + timedelta(days=3650)).isoformat())
        self.raw = b"<record><id>1</id></record>"
        self.version = self.store.ingest_version("owner", self.archive["id"], [
            {"path": "records/one.xml", "content_b64": base64.b64encode(self.raw).decode()},
            {"path": "README.txt", "content_b64": base64.b64encode(b"archive readme").decode()},
        ])
        self.copy1 = self.store.add_copy("owner", self.version["id"], "offline-disk-a")["id"]
        self.copy2 = self.store.add_copy("owner", self.version["id"], "offline-disk-b")["id"]

    def tearDown(self):
        self.tmp.cleanup()

    def test_integrity_repair_and_format_migration(self):
        self.store.simulate_corruption("owner", self.copy1, "records/one.xml")
        result = self.store.verify_copy("owner", self.copy1)
        self.assertEqual(result["state"], "healthy")
        self.assertTrue(result["repaired"])
        self.assertEqual(result["corrupt_paths"], ["records/one.xml"])
        migrated = self.store.migrate(
            "owner", self.version["id"], "records/one.xml", "records/one.html", "html",
            base64.b64encode(b"<html><body><p>1</p></body></html>").decode(),
        )
        detail = self.store.get_version("owner", migrated["id"])
        self.assertEqual(detail["version"]["version"], 2)
        self.assertTrue(any(f["path"] == "records/one.html" for f in detail["files"]))
        status = self.store.archive_status("owner", self.archive["id"])
        self.assertGreater(status["days_remaining"], 3000)

    def test_restricted_access_and_invalid_manifest_are_rejected(self):
        with self.assertRaises(BusinessError) as ctx:
            self.store.get_version("outsider", self.version["id"])
        self.assertEqual(ctx.exception.status, 403)
        with self.assertRaises(BusinessError) as ctx:
            self.store.ingest_version("owner", self.archive["id"], [{"path": "../escape.txt", "content_b64": "eA=="}])
        self.assertEqual(ctx.exception.code, "unsafe_path")
        with self.assertRaises(BusinessError) as ctx:
            self.store.add_copy("owner", self.version["id"], "offline-disk-a")
        self.assertEqual(ctx.exception.code, "copy_exists")


class TransferTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = PreservationStore(Path(self.tmp.name) / "test.db")
        self.store.seed()
        self.archive = self.store.create_archive("owner", "城市测绘档案", (date.today() + timedelta(days=3650)).isoformat())
        self.raw = b"<record><id>1</id></record>"
        self.version = self.store.ingest_version("owner", self.archive["id"], [
            {"path": "records/one.xml", "content_b64": base64.b64encode(self.raw).decode()},
            {"path": "README.txt", "content_b64": base64.b64encode(b"archive readme").decode()},
        ])
        self.copy1 = self.store.add_copy("owner", self.version["id"], "offline-disk-a")["id"]
        self.copy2 = self.store.add_copy("owner", self.version["id"], "offline-disk-b")["id"]

    def tearDown(self):
        self.tmp.cleanup()

    def test_initiate_freezes_and_confirm_transfers_custody(self):
        t = self.store.initiate_transfer("owner", self.archive["id"], "archivist")
        self.assertEqual(t["status"], "pending")
        self.assertEqual(t["total_count"], 2)
        # 发起后原机构仍可查看，但暂时不能新建版本
        self.store.get_version("owner", self.version["id"])
        with self.assertRaises(BusinessError) as ctx:
            self.store.ingest_version("owner", self.archive["id"], [{"path": "x.txt", "content_b64": "eA=="}])
        self.assertEqual(ctx.exception.code, "transfer_in_progress")
        # 接收机构可查看
        self.store.get_version("archivist", self.version["id"])
        # 后到的批次看到冲突
        with self.assertRaises(BusinessError) as ctx:
            self.store.initiate_transfer("owner", self.archive["id"], "auditor")
        self.assertEqual(ctx.exception.code, "transfer_conflict")
        # 全部副本验过才更换保管权
        r = self.store.confirm_transfer("archivist", t["id"])
        self.assertEqual(r["status"], "completed")
        self.assertEqual(r["confirmed_count"], 2)
        self.assertEqual(self.store.archive_status("archivist", self.archive["id"])["archive"]["owner_id"], "archivist")

    def test_corrupt_copy_invalidates_batch_with_blockers(self):
        t = self.store.initiate_transfer("owner", self.archive["id"], "archivist")
        self.store.simulate_corruption("owner", self.copy1, "records/one.xml")
        self.store.simulate_corruption("owner", self.copy2, "records/one.xml")
        r = self.store.confirm_transfer("archivist", t["id"])
        self.assertEqual(r["status"], "invalidated")
        self.assertTrue(any("records/one.xml" in b for b in r["blocking"]))
        # 失效后冻结解除，可继续新建版本
        self.store.ingest_version("owner", self.archive["id"], [{"path": "z.txt", "content_b64": "eg=="}])

    def test_retention_change_invalidates_batch(self):
        t = self.store.initiate_transfer("owner", self.archive["id"], "archivist")
        r = self.store.update_retention("owner", self.archive["id"], (date.today() + timedelta(days=30)).isoformat())
        self.assertTrue(r["invalidated"])
        self.assertTrue(any("保留期限" in b for b in r["blockers"]))
        self.assertEqual(self.store.get_transfer("owner", t["id"])["status"], "invalidated")

    def test_backfill_upgrades_legacy_archive_to_single_item_batch(self):
        with self.store.connect() as conn:
            conn.execute("DELETE FROM transfer_copies WHERE transfer_id IN (SELECT id FROM transfers WHERE archive_id=?)", (self.archive["id"],))
            conn.execute("DELETE FROM transfers WHERE archive_id=?", (self.archive["id"],))
        self.assertEqual(self.store.list_transfers("owner", self.archive["id"]), [])
        self.store.backfill_transfers()
        transfers = self.store.list_transfers("owner", self.archive["id"])
        self.assertEqual(len(transfers), 1)
        self.assertEqual(transfers[0]["status"], "completed")
        self.assertEqual(transfers[0]["confirmed_count"], 2)
        # 之后照常查看和校验
        self.store.get_version("owner", self.version["id"])
        self.store.verify_copy("owner", self.copy1)


if __name__ == "__main__":
    unittest.main()
