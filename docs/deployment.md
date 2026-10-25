# 部署与配置

推荐 Docker Compose。单个进程管理一个账号，Compose 使用私有卷保存凭据与缓存。

## 常用参数

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `BILIMCP_PORT` | `8000` | Compose 映射到本机的端口 |
| `FXBILIBILI_HOST` | `127.0.0.1` | Python 进程监听地址；Compose 内设置为 `0.0.0.0` |
| `FXBILIBILI_PORT` | `8000` | Python 进程监听端口 |
| `FXBILIBILI_ALLOWED_HOSTS` | 空 | 额外允许的精确 Host，逗号分隔，可包含端口 |
| `FXBILIBILI_CREDENTIALS_FILE` | 当前目录 `data/credentials.json` | 私有登录状态绝对路径 |
| `FXBILIBILI_QR_CODE_FILE` | 当前目录 `data/login-qr.png` | 扫码期间的临时二维码绝对路径 |
| `FXBILIBILI_HISTORY_FILE` | 空 | 公开查询 SQLite 绝对路径；为空时不跨进程持久化 |
| `FXBILIBILI_VIDEO_CACHE_FILE` | 随媒体缓存路径确定 | 视频字节块 SQLite 绝对路径 |
| `FXBILIBILI_UPSTREAM_CONCURRENCY` | `3` | 同时执行的 API 请求数 |
| `FXBILIBILI_UPSTREAM_REQUESTS_PER_CALL` | `64` | 每次 MCP 调用的实际 API 发送上限 |
| `FXBILIBILI_UPSTREAM_REQUESTS_PER_MINUTE` | `0` | 每分钟请求限制，零表示关闭此限制 |
| `FXBILIBILI_UPSTREAM_SPACING_SECONDS` | `0.1` | 发送间隔，另加少量抖动 |
| `FXBILIBILI_UPSTREAM_ENDPOINT_LIMITS` | `{}` | 可选 JSON，按 API 路径设置每分钟请求数 |
| `FXBILIBILI_CACHE_MAX_ITEMS` | `1000000` | 内存缓存条目上限 |
| `FXBILIBILI_CACHE_MAX_BYTES` | `268435456` | 内存缓存估算字节上限 |
| `FXBILIBILI_HISTORY_MAX_BYTES` | `4294967296` | 公开查询持久缓存容量 |
| `FXBILIBILI_HISTORY_TTL_SECONDS` | `1209600` | 内容搜索索引有效期，最长 30 天 |
| `FXBILIBILI_VIDEO_CACHE_MAX_BYTES` | `21474836480` | 视频字节块持久缓存容量 |
| `FXBILIBILI_VIDEO_CACHE_TTL_SECONDS` | `604800` | 视频字节块有效期，最长 30 天 |
| `FXBILIBILI_DEBUG_LOG_HTTP` | `false` | 临时排查 HTTP 交换；可能记录查询和公开内容 |

Compose 已设置容器路径。新增运行参数时，需在服务的 `environment` 中显式加入。
`.env` 中未被 Compose 引用的变量不会自动进入容器。

## HTTP 路由

- `POST /mcp`：MCP Streamable HTTP。
- `GET /healthz`：进程健康检查。
- `GET /readyz`：脱敏后的登录、签名与缓存状态。

未知路径返回 JSON 404，已知路径使用错误方法返回 405。
空闲期间签名状态过期可能使 `/readyz` 返回 503；用 `/healthz` 与实际只读调用判断运行情况。

## 常见情况

- 连接被拒绝：确认已完成扫码，检查 `docker compose ps` 和服务日志。
- Host 校验失败：在 `FXBILIBILI_ALLOWED_HOSTS` 中增加实际 Host，保留现有校验。
- `credentials_invalid`：检查容器日志中的重新登录请求，使用自己的账号扫码。
- `rate_limited`：等待返回的时间并降低并发。
- `invalid_cursor`：从该工具的第一页重新开始。
- `resource_limit_exceeded`：缩小范围，避免重复请求同一个超限范围。

默认日志只记录工具名、结果、耗时、发送量与请求 ID，不记录工具查询参数。
排查结束后关闭 HTTP debug，并保管好本地日志。
