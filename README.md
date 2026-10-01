# 数字档案长期保存服务

仅使用 Python 3.11+ 标准库实现的独立档案保存项目，支持清单校验、真实 SHA-256 内容校验、多个离线副本、损坏检测与自动修复、格式迁移、保留期限和访问控制，并提供两个保存机构合库时的**移交批次与逐副本核验**。

## 运行

```bash
python3 app.py --init --seed
python3 app.py
```

服务地址为 <http://127.0.0.1:8102>，默认数据库 `preservation.db`。测试：

```bash
python3 -m unittest -v
```

演示用户：`owner`、`archivist`（甲机构 org-a），`owner-b`、`archivist-b`（乙机构 org-b），`auditor`（审计员），`outsider`（无授权）。API 使用 `X-User-Id`。文件通过 Base64 提交，单文件上限 10 MiB；这是为了保持示例自包含，生产部署应换成对象存储和流式上传。

## 机构移交（合库）

档案始终有当前保管机构（`archives.custodian_id`），移交把档案从一个机构交到另一个机构，中途每一步都有可核对的保管状态：

1. `POST /api/archives/{id}/transfers`，body `{"target_institution_id":"org-b"}`：当前保管机构发起移交批次，批次枚举该档案**全部版本的全部副本**作为条目。同一档案同时只允许一个进行中批次（部分唯一索引 + `BEGIN IMMEDIATE`）：两个人同时提交时，先到的生效，后到的收到 `409 transfer_conflict`。
2. 发起后进入冻结期：原机构与接收机构都能查看档案和批次，但**不能新建版本、不能新增或移除副本、不能迁移格式**（`409 transfer_frozen`）。
3. `POST /api/transfers/{batch}/write`（接收机构）：把每个副本的文件写入接收侧暂存区。按条目逐条提交，中途写失败返回 `500 transfer_write_failed` 并**保留断点**；重试时已写入/已确认的条目不重做。可带 `item_id` 只写一份。
4. `POST /api/transfers/{batch}/items/{item}/confirm`（接收机构）：逐份确认，确认时在对方侧重新做 SHA-256/大小/清单核验。任一份验坏则该批次立即 `invalidated` 并记录阻塞项；全部条目确认后才更换保管权（`custodian_id`、`owner_id` 切换到接收机构并收回原机构成员权限）。确认接口幂等。
5. 批次进行中若**任一副本被验坏**（即使随后从健康副本自动修复）或**保留期限发生变化**（`POST /api/archives/{id}/retention`），未完成批次立即失效，`blockers` 列出全部阻塞项；失效后冻结解除。
6. `GET /api/archives/{id}/transfers`、`GET /api/archives/{id}/transfers/{batch}`：双方可查看批次、逐条状态和阻塞项；`GET /api/archives/{id}/status` 中的 `active_transfer` 反映进行中的移交。
7. `GET /api/institutions`：机构清单。
8. `DELETE /api/copies/{id}`：移除副本（冻结期内被拒绝）。

旧版数据库（没有机构和移交表）启动时自动升级：补齐 `institutions`、用户机构、档案保管机构，并为每件缺少移交记录的旧档案补一条**已完成的单件批次**（`kind=legacy`，由系统账号 `system` 确认），之后照常查看和校验。升级幂等，可反复执行。

## 主要接口

- `GET /api/institutions`：机构列表。
- `POST /api/archives`：创建受限档案（保管机构取创建者所在机构）。
- `POST /api/archives/{id}/members`：所有者授予 read/write 权限。
- `POST /api/archives/{id}/versions`：提交文件清单，服务端重新计算哈希和大小。
- `GET /api/versions/{id}`：查看版本、文件清单和副本状态。
- `POST /api/versions/{id}/copies`：创建独立副本内容。
- `DELETE /api/copies/{id}`：移除副本。
- `POST /api/copies/{id}/verify`：校验副本；发现损坏时从健康副本修复；验坏会失效进行中的移交批次。
- `POST /api/copies/{id}/simulate-corruption`：演示/测试介质损坏，仅 owner 或 archivist 可用。
- `POST /api/versions/{id}/migrate`：生成格式迁移后的新版本并保留派生关系。
- `POST /api/archives/{id}/retention`：修改保留期限；进行中的移交批次随之失效。
- `POST/GET /api/archives/{id}/transfers`、`GET /api/archives/{id}/transfers/{batch}`：移交批次。
- `POST /api/transfers/{batch}/write`、`POST /api/transfers/{batch}/items/{item}/confirm`：接收侧写入与逐份确认。
- `GET /api/archives/{id}/status`：保留期限、版本状态、进行中移交和审计记录。

档案路径拒绝绝对路径和 `..`；同一版本副本位置唯一；没有健康副本时版本标记为 `degraded`；所有变更（发起、写入、确认、失效、换保管权、期限变更）写入审计日志。
