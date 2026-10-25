# 依赖与来源

本项目采用 GPL-3.0-or-later。参考实现与资料署名见根目录 NOTICE。
API 资料只用于接口研究，原文档和协议文件未包含在本项目中。

运行环境包括 Python 3.12、uv 和 FFmpeg。Python 依赖的精确版本锁定在 uv.lock。
每个依赖保留自身的许可证；安装包中的许可证文件属于该依赖。
FFmpeg 由 Docker 镜像中的 Debian 包管理器安装，许可与版权文件随包保存在
`/usr/share/doc/ffmpeg/`。对应源码由 Debian 软件包源提供。

Python 运行依赖的许可证清单在发布验证时根据安装包元数据生成，见下表。

| 依赖 | 锁定版本 | 安装包声明的许可证 |
| --- | --- | --- |
| [annotated-types](https://pypi.org/project/annotated-types/0.8.0/) | `0.8.0` | MIT |
| [anyio](https://pypi.org/project/anyio/4.15.1/) | `4.15.1` | MIT |
| [attrs](https://pypi.org/project/attrs/26.1.0/) | `26.1.0` | MIT |
| [certifi](https://pypi.org/project/certifi/2026.10.23/) | `2026.10.23` | Mozilla Public License 2.0 (MPL 2.0) |
| [cffi](https://pypi.org/project/cffi/2.1.1/) | `2.1.1` | MIT-0 |
| [click](https://pypi.org/project/click/8.5.0/) | `8.5.0` | BSD-3-Clause |
| [colorama](https://pypi.org/project/colorama/0.4.6/) | `0.4.6` | BSD License |
| [cryptography](https://pypi.org/project/cryptography/50.0.2/) | `50.0.2` | Apache-2.0 OR BSD-3-Clause |
| [curl-cffi](https://pypi.org/project/curl-cffi/0.16.3/) | `0.16.3` | MIT |
| [google-re2](https://pypi.org/project/google-re2/1.1.20251105/) | `1.1.20251105` | BSD License |
| [h11](https://pypi.org/project/h11/0.16.0/) | `0.16.0` | MIT License |
| [httpcore](https://pypi.org/project/httpcore/1.0.9/) | `1.0.9` | BSD-3-Clause |
| [httpx](https://pypi.org/project/httpx/0.28.1/) | `0.28.1` | BSD License |
| [httpx-sse](https://pypi.org/project/httpx-sse/0.4.3/) | `0.4.3` | MIT |
| [idna](https://pypi.org/project/idna/3.20/) | `3.20` | BSD-3-Clause |
| [jsonschema](https://pypi.org/project/jsonschema/4.26.0/) | `4.26.0` | MIT |
| [jsonschema-specifications](https://pypi.org/project/jsonschema-specifications/2025.9.1/) | `2025.9.1` | MIT |
| [mcp](https://pypi.org/project/mcp/1.28.1/) | `1.28.1` | MIT License |
| [msgpack](https://pypi.org/project/msgpack/1.2.3/) | `1.2.3` | Apache-2.0 |
| [pillow](https://pypi.org/project/pillow/12.3.0/) | `12.3.0` | MIT-CMU |
| [pycparser](https://pypi.org/project/pycparser/3.12/) | `3.12` | BSD-3-Clause |
| [pydantic](https://pypi.org/project/pydantic/2.14.1/) | `2.14.1` | MIT |
| [pydantic-core](https://pypi.org/project/pydantic-core/2.50.1/) | `2.50.1` | MIT |
| [pydantic-settings](https://pypi.org/project/pydantic-settings/2.16.1/) | `2.16.1` | MIT |
| [pyjwt](https://pypi.org/project/pyjwt/2.15.1/) | `2.15.1` | MIT |
| [python-dotenv](https://pypi.org/project/python-dotenv/1.2.4/) | `1.2.4` | BSD-3-Clause |
| [python-multipart](https://pypi.org/project/python-multipart/0.0.32/) | `0.0.32` | Apache-2.0 |
| [pywin32](https://pypi.org/project/pywin32/312/) | `312` | PSF |
| [qrcode](https://pypi.org/project/qrcode/8.2/) | `8.2` | BSD License, Other/Proprietary License |
| [referencing](https://pypi.org/project/referencing/0.37.0/) | `0.37.0` | MIT |
| [rpds-py](https://pypi.org/project/rpds-py/2026.9.1/) | `2026.9.1` | MIT |
| [sse-starlette](https://pypi.org/project/sse-starlette/3.5.1/) | `3.5.1` | BSD-3-Clause |
| [starlette](https://pypi.org/project/starlette/1.10.0/) | `1.10.0` | BSD-3-Clause |
| [typing-extensions](https://pypi.org/project/typing-extensions/4.16.0/) | `4.16.0` | PSF-2.0 |
| [typing-inspection](https://pypi.org/project/typing-inspection/0.4.4/) | `0.4.4` | MIT |
| [uvicorn](https://pypi.org/project/uvicorn/0.54.0/) | `0.54.0` | BSD-3-Clause |
