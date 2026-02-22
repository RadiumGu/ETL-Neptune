# Neptune ETL 性能优化记录

**日期**: 2026-02-22
**操作人**: Kiro CLI

## 问题

Neptune 图数据同步（创建 graph）非常慢，ETL Lambda 每次执行需要 30+ 秒。

## 根因分析

### 1. ETL 代码：逐条串行 HTTP 请求（主因）

原代码对 100 条调用关系产生 ~700+ 次独立 Neptune HTTP 请求：

| 操作 | 请求数 |
|------|--------|
| upsert 节点（每条边 2 个） | ~200 |
| get_vertex_id（每条边查 2 次 ID） | ~200 |
| upsert 边 | ~100 |
| fetch_dependency_attrs（每服务 4 次查询） | ~160 |
| update_microservice_metrics | ~40 |
| **合计** | **~700+** |

每次请求：SigV4 签名 → HTTPS 新建连接 → 发送 → 等待，单次 10-50ms，串行 700 次 = 7-35 秒。

### 2. Neptune 参数未优化

- `neptune_dfe_query_engine = viaQueryHint`（DFE 未全局启用）
- `neptune_result_cache = 0`（结果缓存关闭）

### 3. 安全组

检查 `sg-00590f44d50a19e5f`（neptune-sg），TCP 8182 已开放给 EKS/Lambda/VPC，WebSocket 和 HTTP 共用端口，**无需修改**。

## 修改内容

### 文件变更：`lambda/etl_deepflow/neptune_etl_deepflow.py`

| 优化项 | 之前 | 之后 | 效果 |
|--------|------|------|------|
| 节点写入 | 逐条 mergeV ~200 次 | 批量链式 mergeV，每批 20 个，~10 次 | -190 请求 |
| 边写入 | 先 get_vertex_id (2次) + upsert (1次) = 3次/边 | 直接属性匹配 1次/边，去掉 get_vertex_id | -200 请求 |
| dependency 查询 | 4 次独立查询/服务 | 1 次 project() 合并查询/服务 | -120 请求 |
| HTTP 连接 | 每次 requests.post 新建连接 | requests.Session() 复用连接 | 延迟降低 |
| 数据流 | 边处理边写 | 先收集再批量写 | 逻辑清晰 |

预估：~700+ 请求 → ~180 请求，耗时 30s → 3-5s。

### Neptune 集群参数变更

创建自定义参数组 `petsite-neptune-optimized`（替代 `default.neptune1.4`）：

```
neptune_dfe_query_engine = enabled    # 全局启用 DFE 查询引擎优化
neptune_result_cache = 1              # 启用结果缓存，加速重复查询
```

已关联到 `petsite-neptune` 集群并重启实例生效。

## 部署方式

CDK deploy 因 EventBridge rule `neptune-etl-every-15min` 缺失回滚，改用直接上传：

```bash
# 打包
cd lambda/etl_deepflow
python3 -c "
import zipfile, os
with zipfile.ZipFile('/tmp/etl_deepflow.zip', 'w', zipfile.ZIP_DEFLATED) as zf:
    for root, dirs, files in os.walk('.'):
        dirs[:] = [d for d in dirs if d != '__pycache__']
        for f in files:
            if not f.endswith('.pyc'):
                zf.write(os.path.join(root, f))
"

# 部署
aws lambda update-function-code \
  --function-name neptune-etl-from-deepflow \
  --zip-file fileb:///tmp/etl_deepflow.zip \
  --region ap-northeast-1
```

## 待办

- [ ] 修复 CDK Stack 中缺失的 EventBridge rule `neptune-etl-every-15min`
- [ ] 验证优化后 ETL 执行时间（查看 CloudWatch Logs）
- [ ] 考虑后续用 WebSocket 长连接替代 HTTP（进一步优化）

---

## 第二批优化：neptune-etl-from-aws + neptune-etl-from-cfn

**日期**: 2026-02-22 21:10

### neptune-etl-from-aws (`lambda/etl_aws/neptune_etl_aws.py`)

| 修改 | 说明 |
|------|------|
| `requests.Session()` 复用 | 新增 `_get_http_session()`，`neptune_query` 改用 session.post()，避免每次新建 TCP 连接 |

原代码已有 `_frozen_creds` 凭证缓存和 `_vid_cache` 顶点 ID 缓存，无需额外改动。

### neptune-etl-from-cfn (`lambda/etl_cfn/neptune_etl_cfn.py`)

| 修改 | 说明 |
|------|------|
| 凭证缓存 | 新增 `_frozen_creds` + `_get_creds()`，原代码每次 `neptune_query` 都新建 boto3 Session |
| `requests.Session()` 复用 | 同上 |
| `_vid_cache` 顶点 ID 缓存 | 新增全局缓存，避免同一顶点被重复查询 |
| `get_or_create_vertex` 改用 `mergeV` | 原来先 get 再 create（2 次请求），改为 mergeV 一步完成（1 次请求），结果写入 `_vid_cache` |
| 删除 `get_vertex_id_by_name` | 不再需要，功能已合并到 `get_or_create_vertex` |

预估效果（2 个 CFN stack，~20 条依赖）：
- 请求数：~100 → ~50（每个顶点从 2 次降到 1 次）
- 单次请求延迟：~50ms → ~15ms（凭证缓存 + 连接复用）

### 部署

```bash
aws lambda update-function-code --function-name neptune-etl-from-aws \
  --zip-file fileb:///tmp/etl_aws.zip --region ap-northeast-1

aws lambda update-function-code --function-name neptune-etl-from-cfn \
  --zip-file fileb:///tmp/etl_cfn.zip --region ap-northeast-1
```
