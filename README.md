# quant-udf

A minimal Python **TradingView UDF (Universal Data Feed)** server that uses the official **掘金 SDK (`gm`)** as the market-data backend.

This project is intended as a template: it provides a working UDF server and a small adapter layer that you can customize to connect to your 掘金 数据源。

---

## 🚀 项目亮点

- ✅ 完整实现 TradingView UDF 协议（`/config`, `/symbols`, `/search`, `/history`, `/time`）
- ✅ 依赖 FastAPI，支持高性能异步 HTTP 服务
- ✅ 使用掘金官方 `gm` SDK 直接拉取 K线和标的信息
- ✅ 未配置 token 时自动使用“stub 数据”，方便本地调试

---

## 🧩 目录结构

- `app.py`：启动 FastAPI 服务
- `udf_service/server.py`：TradingView UDF 接口实现
- `udf_service/juejin_client.py`：掘金 SDK 适配层（可扩展/替换）
- `udf_service/models.py`：TradingView UDF 请求/响应结构

---

## ▶️ 快速启动

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
python -m pip install -r requirements.txt

# 运行服务
python -m app
```

访问示例：

- `http://localhost:8000/config`
- `http://localhost:8000/time`
- `http://localhost:8000/search?query=BTC`
- `http://localhost:8000/history?symbol=SHSE.600000&resolution=D&from=1700000000&to=1701000000`

---

## 🔧 配置要求（必需）

### 环境变量

| 变量 | 含义 | 是否必需 |
|------|------|---------|
| `GM_TOKEN` | 掘金 SDK 认证 Token | ✅ 必需 |

例如：

```bash
export GM_TOKEN="your-token-here"
```

### 默认配置文件（仅 GM_TOKEN）

当环境变量 `GM_TOKEN` 未设置时，服务会按顺序读取以下默认配置文件中的 `GM_TOKEN`：

1. `gm_config.json`（项目根目录）
2. `config/gm_config.json`

示例内容：

```json
{
  "GM_TOKEN": "your-token-here"
}
```

> 读取优先级：**环境变量 `GM_TOKEN` > 默认配置文件中的 `GM_TOKEN`**。  
> ⚠️ 如果两者都没有 `GM_TOKEN`，服务仍会启动，但会返回模拟（stub）数据，无法用于真实行情。

---

## ✅ TradingView 数据源配置（示例）

在 TradingView 的 `Datafeed` 配置中：

```js
const datafeedUrl = "http://localhost:8000";
```

完成后，TradingView 会自动请求：

- `/config`：获取支持的分辨率等能力
- `/symbols`：查询标的元信息
- `/history`：拉取历史 K 线

---

## 🛠 如何定制（接入掘金 SDK）

### 1) 主要入口：`udf_service/juejin_client.py`

- `JuejinClient.get_history(symbol, resolution, from_ts, to_ts)`
  - 负责调用 `gm.api.query.history(...)` 并将结果转成 TradingView 需要的：`t/o/h/l/c/v`
- `JuejinClient.symbols()`
  - 可以调用 `gm.api.query.get_instruments(...)`，返回可搜索的标的列表

### 2) 支持的分辨率映射

当前实现支持：`1、5、15、30、60、D、W`，并将其映射为掘金 SDK 的频率（如 `1m`、`1d`）

---

## 🧪 本地调试 Tips

- 先运行 `curl http://localhost:8000/config` 验证服是否起起来了
- 如返回 `GM_TOKEN is not set`，说明环境变量未生效
- 如果 `history` 返回 `s=error`，请检查：
  - `GM_TOKEN` 是否有效
  - `GM_TOKEN` 是否已通过环境变量或默认配置文件正确加载

---

## 📄 许可证

MIT
