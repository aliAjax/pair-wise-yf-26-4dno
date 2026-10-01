import base64
import hashlib
import tempfile
import threading
import unittest
from datetime import date, timedelta
from pathlib import Path

from app import BusinessError, PreservationStore


class TransferTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = PreservationStore(Path(self.tmp.name) / "test.db")
        self.store.seed()
        self.archive = self.store.create_archive("owner", "城市测绘档案", (date.today() + timedelta(days=3650)).isoformat())
        self.aid = self.archive["id"]
        self.raw = b"<record><id>1</id></record>"
        self.version = self.store.ingest_version("owner", self.aid, [
            {"path": "records/one.xml", "content_b64": base64.b64encode(self.raw).decode()},
            {"path": "README.txt", "content_b64": base64.b64encode(b"archive readme").decode()},
        ])
        self.vid = self.version["id"]
        self.copy1 = self.store.add_copy("owner", self.vid, "offline-disk-a")["id"]
        self.copy2 = self.store.add_copy("owner", self.vid, "offline-disk-b")["id"]

    def tearDown(self):
        self.tmp.cleanup()

    def _start(self):
        return self.store.start_transfer("owner", self.aid, "org-b")

    def _full_confirm(self, batch_id, items, user="archivist-b"):
        self.store.write_transfer(user, batch_id)
        results = []
        for item in items:
            results.append(self.store.confirm_transfer_item(user, batch_id, item["id"]))
        return results


class TransferFlowTests(TransferTestBase):
    def test_full_handoff_flips_custody_only_after_all_confirmed(self):
        batch = self._start()
        self.assertEqual(batch["state"], "pending")
        self.assertEqual(batch["total"], 2)
        items = batch["items"]

        # 逐份确认：确认第一份后保管权仍属于原机构
        written = self.store.write_transfer("archivist-b", batch["id"])
        self.assertEqual(sorted(written["written"]), sorted(i["id"] for i in items))
        first = self.store.confirm_transfer_item("archivist-b", batch["id"], items[0]["id"])
        self.assertEqual(first["confirmed"], 1)
        status = self.store.archive_status("owner", self.aid)
        self.assertEqual(status["archive"]["custodian_id"], "org-a")
        self.assertIsNotNone(status["active_transfer"])

        last = self.store.confirm_transfer_item("archivist-b", batch["id"], items[1]["id"])
        self.assertEqual(last["state"], "completed")
        self.assertEqual(last["completed"]["custodian_id"], "org-b")
        status = self.store.archive_status("owner-b", self.aid)
        self.assertEqual(status["archive"]["custodian_id"], "org-b")
        self.assertIsNone(status["active_transfer"])

    def test_source_can_read_but_cannot_change_during_pending(self):
        batch = self._start()
        # 原机构仍能查看版本和批次
        self.store.get_version("owner", self.vid)
        self.store.get_transfer("owner", batch["id"])
        # 暂时不能新建版本
        with self.assertRaises(BusinessError) as ctx:
            self.store.ingest_version("owner", self.aid, [
                {"path": "new.txt", "content_b64": base64.b64encode(b"x").decode()}])
        self.assertEqual(ctx.exception.code, "transfer_frozen")
        # 不能移除副本
        with self.assertRaises(BusinessError) as ctx:
            self.store.remove_copy("owner", self.copy2)
        self.assertEqual(ctx.exception.code, "transfer_frozen")
        # 不能新增副本（冻结期副本集合必须稳定）
        with self.assertRaises(BusinessError) as ctx:
            self.store.add_copy("owner", self.vid, "offline-disk-c")
        self.assertEqual(ctx.exception.code, "transfer_frozen")
        # 接收机构在确认前无权新建版本
        with self.assertRaises(BusinessError) as ctx:
            self.store.ingest_version("archivist-b", self.aid, [
                {"path": "new.txt", "content_b64": base64.b64encode(b"x").decode()}])
        self.assertEqual(ctx.exception.code, "transfer_frozen")
        # 无关节点看不到
        with self.assertRaises(BusinessError):
            self.store.get_transfer("outsider", batch["id"])

    def test_receiver_cannot_write_or_confirm(self):
        batch = self._start()
        item_id = batch["items"][0]["id"]
        with self.assertRaises(BusinessError) as ctx:
            self.store.write_transfer("owner", batch["id"])
        self.assertEqual(ctx.exception.status, 403)
        with self.assertRaises(BusinessError) as ctx:
            self.store.confirm_transfer_item("owner", batch["id"], item_id)
        self.assertEqual(ctx.exception.status, 403)

    def test_concurrent_start_first_wins(self):
        barrier = threading.Barrier(2)
        outcomes = []

        def submit():
            barrier.wait()
            try:
                outcomes.append(("ok", self.store.start_transfer("owner", self.aid, "org-b")["id"]))
            except BusinessError as exc:
                outcomes.append(("conflict", exc.code))

        t1 = threading.Thread(target=submit)
        t2 = threading.Thread(target=submit)
        t1.start(); t2.start(); t1.join(); t2.join()
        self.assertEqual(sorted(o[0] for o in outcomes), ["conflict", "ok"])
        loser_code = outcomes[0][1] if outcomes[1][0] == "ok" else outcomes[1][1]
        self.assertIn(loser_code, {"transfer_conflict", "transfer_frozen"})
        pending = [b for b in self.store.list_transfers("owner", self.aid)["batches"] if b["state"] == "pending"]
        self.assertEqual(len(pending), 1)

    def test_corrupt_copy_invalidates_pending_batch_with_blockers(self):
        batch = self._start()
        self.store.write_transfer("archivist-b", batch["id"])
        # 源侧副本验坏（可自动修复也要失效）
        self.store.simulate_corruption("owner", self.copy1, "records/one.xml")
        result = self.store.verify_copy("owner", self.copy1)
        self.assertTrue(result["repaired"])
        self.assertEqual(result["invalidated_batches"], [batch["id"]])
        view = self.store.get_transfer("owner-b", batch["id"])
        self.assertEqual(view["state"], "invalidated")
        self.assertTrue(view["blockers"])
        self.assertIn("copy_id", view["blockers"][0])
        # 失效后继续写入被拒绝
        with self.assertRaises(BusinessError) as ctx:
            self.store.write_transfer("archivist-b", batch["id"])
        self.assertEqual(ctx.exception.code, "batch_not_pending")
        # 失效后冻结解除
        self.store.ingest_version("owner", self.aid, [
            {"path": "v2.txt", "content_b64": base64.b64encode(b"v2").decode()}])

    def test_receiver_side_bad_staging_invalidates(self):
        batch = self._start()
        self.store.write_transfer("archivist-b", batch["id"])
        # 直接篡改接收侧暂存内容，模拟对方介质上的损坏
        item = batch["items"][0]
        with self.store.connect() as conn:
            row = conn.execute(
                "SELECT content FROM transfer_item_files WHERE item_id=? AND path='records/one.xml'",
                (item["id"],)).fetchone()
            damaged = bytes([row["content"][0] ^ 0x01]) + row["content"][1:]
            conn.execute(
                "UPDATE transfer_item_files SET content=? WHERE item_id=? AND path='records/one.xml'",
                (damaged, item["id"]))
        with self.assertRaises(BusinessError) as ctx:
            self.store.confirm_transfer_item("archivist-b", batch["id"], item["id"])
        self.assertEqual(ctx.exception.code, "verify_failed")
        view = self.store.get_transfer("archivist-b", batch["id"])
        self.assertEqual(view["state"], "invalidated")
        self.assertTrue(any("验坏" in b["reason"] for b in view["blockers"]))

    def test_retention_change_invalidates_pending_batch(self):
        batch = self._start()
        new_date = (date.today() + timedelta(days=4000)).isoformat()
        result = self.store.update_retention("owner", self.aid, new_date)
        self.assertEqual(result["invalidated_batches"], [batch["id"]])
        view = self.store.get_transfer("owner", batch["id"])
        self.assertEqual(view["state"], "invalidated")
        self.assertTrue(any("new_retention_until" in b for b in view["blockers"]))
        # 同日期重复更新不再失效任何批次
        again = self.store.update_retention("owner", self.aid, new_date)
        self.assertEqual(again["invalidated_batches"], [])

    def test_write_keeps_checkpoint_and_retries_skip_done(self):
        batch = self._start()
        items = batch["items"]
        # 第一份写完后注入故障
        with self.assertRaises(BusinessError) as ctx:
            self.store.write_transfer("archivist-b", batch["id"], fail_after=1)
        self.assertEqual(ctx.exception.code, "transfer_write_failed")
        states = {i["seq"]: i["state"] for i in self.store.get_transfer("archivist-b", batch["id"])["items"]}
        self.assertEqual(states[1], "written")
        self.assertEqual(states[2], "pending")
        # 已确认的条目不重做：先确认第 1 份，再重试
        self.store.confirm_transfer_item("archivist-b", batch["id"], items[0]["id"])
        retry = self.store.write_transfer("archivist-b", batch["id"])
        self.assertEqual(retry["written"], [items[1]["id"]])
        self.assertEqual(retry["skipped"], [])
        self.store.confirm_transfer_item("archivist-b", batch["id"], items[1]["id"])
        self.assertEqual(self.store.get_transfer("owner-b", batch["id"])["state"], "completed")

    def test_legacy_archives_upgraded_to_singleton_batch(self):
        # 旧档案升级后仍可查看和校验
        listing = self.store.list_transfers("owner", self.aid)["batches"]
        legacy = [b for b in listing if b["kind"] == "legacy"]
        self.assertEqual(len(legacy), 1)
        self.assertEqual(legacy[0]["state"], "completed")
        self.assertEqual(legacy[0]["total"], 1)
        self.assertEqual(legacy[0]["items"][0]["state"], "confirmed")
        # 升级是幂等的：不会重复补批次
        self.store.archive_status("owner", self.aid)
        self.store.list_transfers("owner", self.aid)
        self.assertEqual(len(self.store.list_transfers("owner", self.aid)["batches"]), 1)
        # 旧副本照常校验
        self.assertEqual(self.store.verify_copy("owner", self.copy1)["state"], "healthy")

    def test_after_completion_source_loses_access_receiver_controls(self):
        batch = self._start()
        items = self.store.get_transfer("owner", batch["id"])["items"]
        self._full_confirm(batch["id"], items)
        # 原机构成员失去访问
        with self.assertRaises(BusinessError) as ctx:
            self.store.get_version("owner", self.vid)
        self.assertEqual(ctx.exception.status, 403)
        # 接收机构可查看、可新建版本
        self.store.get_version("owner-b", self.vid)
        self.store.ingest_version("owner-b", self.aid, [
            {"path": "v2.txt", "content_b64": base64.b64encode(b"v2").decode()}])
        # 审计员仍可审计
        self.store.archive_status("auditor", self.aid)

    def test_confirm_without_write_rejected_and_idempotent(self):
        batch = self._start()
        item = batch["items"][0]
        with self.assertRaises(BusinessError) as ctx:
            self.store.confirm_transfer_item("archivist-b", batch["id"], item["id"])
        self.assertEqual(ctx.exception.code, "item_not_written")
        self.store.write_transfer("archivist-b", batch["id"], item_id=item["id"])
        first = self.store.confirm_transfer_item("archivist-b", batch["id"], item["id"])
        self.assertEqual(first["confirmed"], 1)
        again = self.store.confirm_transfer_item("archivist-b", batch["id"], item["id"])
        self.assertTrue(again.get("idempotent"))


if __name__ == "__main__":
    unittest.main()
