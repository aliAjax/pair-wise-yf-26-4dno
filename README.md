# 数字档案长期保存服务

仅使用 Python 3.11+ 标准库实现的独立档案保存项目，支持清单校验、真实 SHA-256 内容校验、多个离线副本、损坏检测与自动修复、格式迁移、保留期限和访问控制。

## 运行

```bash
python3 app.py --init --seed
python3 app.py
```

服务地址为 <http://127.0.0.1:8102>，默认数据库 `preservation.db`。测试：

```bash
python3 -m unittest -v
```

演示用户：`owner`、`archivist`、`auditor`、`outsider`。API 使用 `X-User-Id`。文件通过 Base64 提交，单文件上限 10 MiB；这是为了保持示例自包含，生产部署应换成对象存储和流式上传。

## 主要接口

- `POST /api/archives`：创建受限档案。
- `POST /api/archives/{id}/members`：所有者授予 read/write 权限。
- `POST /api/archives/{id}/versions`：提交文件清单，服务端重新计算哈希和大小。
- `GET /api/versions/{id}`：查看版本、文件清单和副本状态。
- `POST /api/versions/{id}/copies`：创建独立副本内容。
- `POST /api/copies/{id}/verify`：校验副本；发现损坏时从健康副本修复。
- `POST /api/copies/{id}/simulate-corruption`：演示/测试介质损坏，仅 owner 或 archivist 可用。
- `POST /api/versions/{id}/migrate`：生成格式迁移后的新版本并保留派生关系。
- `GET /api/archives/{id}/status`：保留期限、版本状态和审计记录。
- `POST /api/archives/{id}/transfers`：发起机构合库移交（body 为 `{"to_owner_id": "..."}`），同一档案同时只能有一个未完成批次，后到者返回 `transfer_conflict`。
- `GET /api/archives/{id}/transfers`、`GET /api/transfers/{id}`：查看移交批次与逐份确认状态。
- `POST /api/transfers/{id}/confirm`：接收机构逐份确认副本；全部副本验过才更换保管权（`owner_id` 变更）。
- `POST /api/archives/{id}/retention`：变更保留期限；若存在未完成移交批次，批次立即失效并返回阻塞项。
- `POST /api/copies/{id}/remove`：移除副本（移交冻结期内不可用）。

移交规则：发起后原机构仍可查看，但暂时不能新建版本、新增或移除副本；接收机构在移交期间可查看和校验副本。任一副本验坏或保留期限变化，未完成批次立即失效并列出阻塞项。确认按副本逐份提交，中途失败后重试只处理未确认的副本，已确认的不重做。旧数据缺少移交批次时会在初始化时升级为单件批次（保管权归属当前机构、副本视为已确认），之后照常查看和校验。

档案路径拒绝绝对路径和 `..`；同一版本副本位置唯一；没有健康副本时版本标记为 `degraded`；所有变更写入审计日志。
