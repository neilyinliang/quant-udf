# quant-udf 代码审查报告

> 范围：`app.py`、`udf_service/*.py`、`scripts/gm_ws_worker.py`、`pyproject.toml`  
> 目标：找出 Bug、性能改进点、以及更好的实现写法。  
> 结论：当前系统主链路可用；**P0 项已完成**（UDF 语义与打包配置关键问题已修复），当前主要剩余为可维护性与性能优化项。

---

## 1. 总体结论（Executive Summary）

- ✅ **优点**
  - HTTP UDF 基础接口完整（`/config`, `/symbols`, `/search`, `/history`, `/time`）。
  - WebSocket 实时链路已可运行，支持主连合约解析（`DCE.l -> DCE.l2605`）。
  - Realtime worker 进程隔离架构方向正确（避免在主进程直接跑 gm runtime 的线程/信号问题）。

- ⚠️ **当前主要问题（P0 后）**
  1. `realtime_ws.py` 中仍保留一份“旧版 worker 逻辑”，与 `scripts/gm_ws_worker.py` 存在双实现漂移风险。
  2. 高频路径仍有优化空间（符号列表、主连解析、消息路由与广播策略）。
  3. 可观测性与协议建模仍可增强（错误分类、指标、Typed 模型）。

---

## 2. Bug 列表（按优先级）

## 2.1 [高][已修复] `/history` 对 `no_data` 语义处理错误

- **位置**：`udf_service/server.py` `history()`  
- **修复结果**：已改为 `s in {"ok","no_data"}` 返回 200，只有错误态返回 5xx。  
- **当前状态**：已通过。

---

## 2.2 [高][已修复] `pyproject.toml` 依赖声明不规范

- **位置**：`pyproject.toml`  
- **修复结果**：已改为标准 PEP 621 `dependencies = []`，并使用 PEP 440 版本范围。  
- **当前状态**：已通过。

---

## 2.3 [中] `realtime_ws.py` 中残留旧 worker 实现（未被主流程使用）

- **位置**：`udf_service/realtime_ws.py` 的 `_worker_start()` 及其内嵌回调  
- **现状**：主流程已迁移到 `scripts/gm_ws_worker.py` 子进程，但文件内仍有另一套 worker 回调实现。
- **问题**：双实现造成行为漂移，后续修 bug 容易改一处漏一处。
- **影响**：维护复杂度上升，未来引入隐藏回归。
- **建议修复**：删除或明确废弃该旧实现，仅保留“启动外部 worker”的单一路径。

---

## 2.4 [中][已修复] `/time` 语义与兼容性

- **位置**：`udf_service/server.py` `/time`  
- **修复结果**：已改为返回纯文本 UNIX 秒时间戳，并统一注释说明。  
- **当前状态**：已通过。

---

## 2.5 [中][已修复] `countback` 参数未使用

- **位置**：`udf_service/server.py` `history()`  
- **修复结果**：已实现 `countback` 对 `t/o/h/l/c/v` 的统一尾部截断逻辑。  
- **当前状态**：已通过。

---

## 2.6 [中][已修复] `SearchResult.exchange` 字段映射不准确

- **位置**：`udf_service/server.py`、`udf_service/models.py`、`udf_service/juejin_client.py`  
- **修复结果**：已新增并填充 `SymbolInfo.exchange`，`/search` 改为映射 `exchange=s.exchange`。  
- **当前状态**：已通过。

---

## 2.7 [低][已修复] `SymbolInfo.supported_resolutions` 可变默认值风险

- **位置**：`udf_service/models.py`  
- **修复结果**：已改为 `Field(default_factory=...)`。  
- **当前状态**：已通过。

---

## 2.8 [低] 仍存在 tick 相关分支/字段判断残留

- **位置**：`udf_service/realtime_ws.py`（例如 reader 中 `mtype == "tick"`、bar 路由中 `sub.frequency == "tick"`）  
- **现状**：当前已禁用 tick，但保留兼容分支。
- **问题**：语义噪音，增加心智负担。
- **建议修复**：若确认长期 bar-only，可清理残留分支。

---

## 3. 性能优化建议（按收益排序）

## 3.1 对 `symbols()` 增加短 TTL 缓存（高收益）

- **现状**：`/search` 与 `/symbols` 频繁调用 `query.get_instruments(df=True)`，成本高。
- **建议**：在 `JuejinClient` 内加 30~120 秒缓存（可配置）。
- **收益**：显著降低外部 SDK 调用与 DataFrame 构造开销。

---

## 3.2 对主连解析 `_resolve_main_contract_symbol()` 做缓存（高收益）

- **现状**：每次订阅/查询都可能调用 `fut_get_continuous_contracts`。
- **建议**：按 symbol 做短 TTL 缓存（例如 10~60 秒），并加失败缓存（negative cache）。
- **收益**：降低主连解析压力，提升实时订阅吞吐。

---

## 3.3 WebSocket 广播路径减少每客户端 Future 创建（中收益）

- **现状**：每条消息对每个客户端 `run_coroutine_threadsafe`，Future 数量大。
- **建议**：
  - 在事件循环线程内统一批量 `create_task`；
  - 或构建发送队列/批处理机制。
- **收益**：降低高并发下调度与对象分配开销。

---

## 3.4 订阅路由索引化（中收益）

- **现状**：`_deliver_bar_message()` 遍历全量 `clients * subs`。
- **建议**：维护索引：`(resolved_symbol, frequency) -> set[ws]`。
- **收益**：消息分发从 O(N*M) 收敛到接近 O(K)。

---

## 3.5 常量外提与热点路径减分配（低~中收益）

- **现状**：频率 alias map 在函数内重复创建。
- **建议**：外提为模块级常量，减少临时对象创建。

---

## 4. 更好的写法（可维护性/一致性）

## 4.1 提取共享配置读取模块

- **现状**：`GM_TOKEN` 读取逻辑在多个文件重复。
- **建议**：提取到 `udf_service/config.py`，统一日志、优先级和错误策略。

---

## 4.2 用明确的协议模型替代裸 dict

- **现状**：WS 收发消息多为裸 dict + 字符串常量。
- **建议**：定义 Pydantic 模型或 TypedDict（`SubscribeCmd`, `BarEvent`, `AckEvent`）。
- **收益**：减少字段拼写错误，提升重构安全性。

---

## 4.3 `history` 响应路径按 `s` 分类处理

建议逻辑：
- `s == "ok"`：200 返回完整数组；
- `s == "no_data"`：200 返回 no_data 结构（含 `next_time` 可选）；
- `s == "error"`：502/500；
- 其他值：记录错误并按 `error` 处理。

---

## 4.4 清理“僵尸代码”和注释一致性

- 删除 `realtime_ws.py` 内未使用 worker 回调实现；
- 统一注释中的协议示例、时间单位、支持频率；
- 删除已禁用功能的历史描述，避免误导。

---

## 4.5 错误分类与可观测性增强

- 给 WS 错误增加分类码（如 `ERR_WORKER_DOWN`, `ERR_BAD_CMD`）；
- 给 worker 状态增加健康指标（订阅数、最后 bar 时间）；
- 对关键路径打点（订阅耗时、消息吞吐）。

---

## 5. 推荐修复优先级（实施计划）

### P0（已完成）
1. 修复 `/history` 对 `no_data` 的处理。
2. 修复 `pyproject.toml` 为标准 PEP 621。
3. 校正 `/time` 语义并提升兼容性（纯文本 UNIX 秒时间戳）。
4. 修复 `exchange` 字段语义映射。
5. 落地 `countback` 截断逻辑。

### P1（当前优先）
1. 清理 `realtime_ws.py` 中旧 worker 实现（消除双实现漂移）。
2. 为 `symbols` 与主连解析加 TTL 缓存。
3. 增补回归测试（`countback`、`/time` 格式、`exchange` 映射）。

### P2（后续）
1. WS 分发索引化与广播批处理优化。
2. 引入协议模型（TypedDict/Pydantic）统一校验。
3. 完善健康检查与运行指标。

---

## 6. 附：建议的验收标准（DoD）

- ✅ `/history` 在无数据区间返回 `200 + {"s":"no_data"}`。
- ✅ `pyproject.toml` 已切换到 PEP 621 规范依赖声明。
- ✅ `/time` 已返回纯文本 UNIX 秒时间戳。
- ✅ `countback` 已实现并生效。
- ✅ `search.exchange` 已按交易所字段语义返回。
- ⏳ 仍建议补齐/强化单测覆盖：
  - `history` 的 `ok/no_data/error` 三态；
  - `countback` 截断边界；
  - `/time` 返回格式；
  - `search` 字段语义；
  - WS bar 路由与频率别名匹配。
- ⏳ 压测下 WS 分发 CPU 占用低于现状基线（建议记录前后对比）。

---

如果你希望，我可以基于这个报告再产出一个 `FIX_PLAN.md`（逐项任务拆解 + 预计工时 + 回归测试清单）。