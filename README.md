# Bilibili 互联

B 站互联，拥有访问 B 站能力。

1.0 提供 25 项只读 MCP 工具，支持视频、字幕、弹幕、评论、Opus、用户资料、
动态、公开收藏夹、视频合集、直播间、公开大航海名单、图片和视频截帧。
使用自己的 B 站账号扫码登录，适合单机 Docker Compose 部署。

## 快速开始

安装 Docker 和 Compose 后运行：

```sh
git clone https://git.ruinet.work/crop/BiliMCP.git
cd BiliMCP
cp .env.example .env
docker compose up -d --build
docker compose logs -f bilimcp
```

首次启动在日志中显示终端二维码。使用 B 站客户端在 60 秒内扫码并确认登录。
二维码过期会使本次启动失败，容器自动重启后显示新的二维码。
登录成功后服务将凭据以 `0600` 权限写入私有卷，重启时自动加载，并按需刷新。

MCP 客户端连接地址：`http://127.0.0.1:8000/mcp`，传输方式选择 Streamable HTTP。
可以在 `.env` 中修改 `BILIMCP_PORT`。服务启动完成后，以下命令应返回 `{"status":"ok"}`：

```sh
curl http://127.0.0.1:8000/healthz
```

如果 MCP 客户端也在容器里，`127.0.0.1` 指向客户端容器自身。
将两者加入同一个私有 Docker 网络后，客户端可使用 `http://bilimcp:8000/mcp`。

服务没有应用内鉴权，默认仅向本机开放。远程访问需自行配置访问控制与 HTTPS。

## 工具与使用

见 [工具说明](docs/tools.md)。首次查询先调用 `get_help(topic="overview")`。
需要某项工具的完整输入输出契约时，调用 `get_help(tool="工具名")`。

文本工具返回结构化 JSON 和相同的文本 JSON。图片与截帧通过原生 `ImageContent` 返回。
原始图片 URL 指向来源 CDN；精确截帧通过客户端支持的原生图片展示。
分页参数按工具返回的 `next_range`、`next_time_range` 或 `next_cursor` 原样续读。

上游风控或验证码挑战会返回 `rate_limited`，并暂停相应端点。
按返回的等待时间重试。登录过期导致的重新扫码会显示在容器日志中。
用户资料来自 Web 接口；`get_user` 不额外请求 App 专用 IP 属地标签。

## 数据与配置

Compose 创建两个独立卷：`credentials` 保存登录状态，`data` 保存公开查询缓存和媒体缓存。
凭据、二维码、日志及数据库均由部署者本地保存，不应提交到代码仓库。

普通缓存使用内存和 SQLite，公开数据默认上限 4 GiB；视频字节块缓存默认上限 20 GiB。
媒体按需下载，视频缓存按 LRU 和七天 TTL 淘汰。游标有效期 30 分钟，进程重启后失效。

`docker compose down` 保留数据卷。`docker compose down -v` 会删除登录状态及缓存，
下次启动需要重新扫码。

更多参数与排查步骤见 [部署说明](docs/deployment.md)。

## 本地运行

需要 Python 3.12、uv 和 FFmpeg：

```sh
uv sync --locked --no-dev
uv run --no-dev bilibili-connect
```

默认监听 `127.0.0.1:8000`，登录文件位于当前目录的 `data/`。
本地运行如需持久查询缓存，请设置部署说明中的绝对路径参数。

## 许可证

[GPL-3.0-or-later](LICENSE)。来源署名见 [NOTICE](NOTICE)，
运行依赖及其许可证见 [依赖说明](docs/dependencies.md)。

## 免责声明

仓库内容来源于互联网公开资源，仅供学习、交流及测试使用。相关内容版权归原权利人所有。

如有侵权，请通过 邮件联系 Rui Feng 反馈，我们将在核实后及时处理。
