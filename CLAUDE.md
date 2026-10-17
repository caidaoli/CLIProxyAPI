# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

AI 工作指南。**必读 Fork-Specific 部分**——合并上游或重构时直接决定取舍。

## Identity

- Module: `github.com/router-for-me/CLIProxyAPI/v8`
- Go 1.26.0
- 上游 `router-for-me/CLIProxyAPI`（远程 `upstream`），当前基于 v8.0.32
- Fork 增量: 冷却状态持久化 · 渐进式凭证加载 · usage 持久化 · 管理中心 · 管理面板资源管理 · uTLS protected-host 降级
- Fork tag 命名: `fork/v*`（如 `fork/v8.20.1`），与上游 `v8.x.x` tag 共存

## Hard Constraints

- `internal/translator/` 是 PR 受保护路径——上游 CI 自动拒绝该目录的 PR。修改需走 issue。
- `fork/v*` tag 同时触发 release 和 docker-image 工作流；构建前同步并校验管理面板资源，多架构镜像推送到 `ghcr.io`（非 DockerHub）。
- 构建参数: `CGO_ENABLED=0`，静态链接，`-trimpath`，版本通过 ldflags 注入。
- 合并上游前必读「Fork-Specific Modifications」清单；任何 fork-only 文件被删/覆盖即为合并错误。

## Architecture

### 请求处理流

```
Client → Gin HTTP Server (internal/api/server.go)
       → Middleware 栈 (logging → compression → CORS → heartbeat → safe-mode)
       → Route Handler (chat/responses/images/videos/management)
       → Translator (internal/translator/ — 请求/响应跨格式转换)
       → Auth Selector (sdk/cliproxy/auth/conductor.go — 凭证编排)
       → Provider Executor (internal/runtime/executor/ — 实际 API 调用)
       → Response streaming back to client
```

### 服务构建与生命周期

`cmd/server/main.go` → `sdk/cliproxy/Builder` (fluent builder 模式) → `Service`

Builder 注入核心组件: TokenStore、AuthManager、AccessManager、PluginHost、Hooks。
`Build()` 根据 `routing` 配置创建对应的 Selector（RoundRobin/FillFirst/WeightedRoundRobin），
可选包装 SessionAffinitySelector。

### Token Store 后端优先级

启动时按优先级检测: PostgreSQL (`PGSTORE_*`) > Object Store (`OBJECTSTORE_*`) > Git Store (`GITSTORE_*`) > 本地文件。
Store 接口统一 `List/Save/Delete`，auth 文件和 config 均可持久化到远程后端。

### 凭证选择与冷却

`auth/conductor.go` 编排多级 provider 轮转。每个 provider 内由 Selector 选择凭证。
凭证触发配额限制后进入冷却状态（`cooldown_state.go`），冷却期间自动跳过。
`save-cooldown-status` 配置项控制冷却状态持久化（支持文件 / PG 后端），重启后恢复。

### Executor 模式

每个 provider 一个 executor 文件，均实现 `ProviderExecutor` 接口。
Executor 负责：构造上游请求、处理认证、流式/非流式转发、错误处理与重试。
`helps/` 子目录提供共享工具（uTLS 客户端、HTTP 传输、重试逻辑）。

### 热重载

`internal/watcher/` 通过 `fsnotify` 监控 config 和 auth 文件变更，变更后自动重载，无需重启。
启动时 auth 文件由有界 worker pool 渐进加载；服务先启动，再持续注册扫描结果。新扫描会取代旧扫描，失败或过期结果不得污染运行态。

## Module Map

| 路径 | 职责 |
|---|---|
| `cmd/server/main.go` | 入口；CLI flags（含 `-tui`）；token store 初始化；`internal/cmd.StartService` / `StartServiceBackground` |
| `sdk/cliproxy/` | 可嵌入 SDK：`service.go` `builder.go` `auth/` `executor/`（含 `Pinned/Selected/ExecutionSessionMetadataKey`） |
| `internal/api/` | Gin HTTP server；`handlers/management/` |
| `internal/runtime/executor/` | Provider executor: Gemini / Gemini-CLI / Gemini-Vertex / Claude / Codex / Codex-WebSocket / Antigravity / Kimi / AIStudio / OpenAI-compat / xAI |
| `internal/translator/` | 请求/响应跨格式转换（受 PR 守卫保护） |
| `internal/thinking/provider/` | 扩展思考/推理 |
| `internal/watcher/` | config/auth 热重载；**fork**：渐进式并行 auth 加载与状态快照 |
| `internal/auth/` | 每 provider OAuth；`xai/` 使用 PKCE |
| `internal/home/` | Home 控制平面：Redis 集群协调、TLS、cluster nodes 解析、Pub/Sub |
| `internal/usage/` | **fork**：SQLite usage 日志、监控查询、PG 镜像 |
| `internal/registry/` | 动态模型注册表 + `models/codex_client_models.json` |
| `internal/tui/` | bubbletea TUI（dashboard/auth/config/keys/logs/oauth/usage） |
| `internal/managementasset/` | **fork**：内嵌管理面板基线、manifest/SHA256 校验、磁盘升级与原子替换 |

## Key Interfaces

```go
// sdk/cliproxy/executor
type ProviderExecutor interface {
    Identifier() string
    Execute(ctx, auth, req, opts) (Response, error)
    ExecuteStream(ctx, auth, req, opts) (<-chan StreamChunk, error)
    Refresh(ctx, auth) (*Auth, error)
}

// sdk/cliproxy/auth
type Selector interface {
    Select(ctx, auths []*Auth, model string, opts SelectOptions) (*Auth, error)
}
type ExecutionSessionCloser interface {
    CloseExecutionSession(sessionID string)
}
```

## Commands

```bash
# 构建
go build -o cli-proxy-api ./cmd/server/
docker build -t cliproxyapi .

# 测试
go test ./...                                              # 全部测试
go test ./sdk/cliproxy/auth/                               # 单包
go test ./sdk/cliproxy/auth/ -run TestRoundRobinSelectorPick # 匹配名称
go test ./sdk/cliproxy/auth/ -run TestNormalize/empty      # 单子测试
go test -race ./...                                        # race 检测
go test -count=1 ./internal/usage/                         # 禁用缓存

# 运行
./cli-proxy-api [-config path] [-tui [-standalone]] [-vertex-import key.json]
# OAuth: -login -codex-login -codex-device-login -claude-login
#        -antigravity-login -kimi-login -xai-login
# Login flags: -oauth-callback-port PORT, -no-browser
```

## Testing Conventions

- **纯标准库 `testing`**——无 testify、gomock 等第三方框架。断言用 `t.Fatalf`/`t.Errorf`。
- **Table-driven tests** + `t.Run` 子测试为主流模式，子测试通常标记 `t.Parallel()`。
- **手写 stub/fake**——手动实现接口或使用函数类型适配器（类似 `http.HandlerFunc`），定义在同包 `_test.go` 中。
- **隔离**: `t.TempDir()` 临时路径、`t.Setenv()` 环境变量、`t.Cleanup()` 清理回调。
- **集成测试**: `test/` 目录，`package test`，使用 `httptest.NewServer` 构建真实依赖。
- 新增测试必须匹配上述风格——不要引入第三方断言库。

## Config

- `config.yaml`（template: `config.example.yaml`）；`auths/*.json`；`.env`
- 配置文件使用 v8 分组布局；旧版顶层字段仍可读取，显式迁移时映射到 `server`、`management`、`routing`、`oauth`、`observability` 等分组。
- 主要 sections: `tls` `remote-management` `routing` `proxy-url` `request-retry` `quota-exceeded` `payload` `oauth-model-alias` `oauth-excluded-models` `oauth-request-scoped-errors` `ws-auth` `usage-persistence-enabled` `delete-unauthorized-auth` `auth-load-workers` `local-model` `claude-header-defaults` `cloak` `passthrough-headers` `streaming` `home` `save-cooldown-status`
- Token store env: 默认本地 file；`PGSTORE_*` / `GITSTORE_*` / `OBJECTSTORE_*`
- `MANAGEMENT_STATIC_PATH`: 覆盖管理面板磁盘升级目录；默认使用 writable path 下的 `static/`，否则回退到 config 所在目录的 `static/`
- `MANAGEMENT_PANEL_DEV_PATH`: 显式开发面板覆盖；设置后直接使用该文件并跳过自动更新
- Routing 策略: `round-robin`（default）· `fill-first`/`ff` · `weighted-round-robin`/`wrr`

## API Routes

| Route | 说明 |
|---|---|
| `/v1/*` | OpenAI 兼容（chat、models） |
| `/v1/images/generations`, `/v1/images/edits` | xAI Grok 等图像 |
| `/v1/videos`, `/v1/videos/{generations,edits,extensions,:request_id}` | xAI Grok 视频 |
| `/v1/responses` POST/GET | Responses API + WS 升级 |
| `/v0/management/*` | 管理 API；增强 auth-files 分页/批量修改/归档导入/过滤删除；panel latest/update；auth load status；`plugins/:id` DELETE |
| `/v0/management/custom/monitor/*` | **fork** 13 个监控端点 |
| `/v0/management/custom/codex-cleanup` | **fork** Codex 认证清理 |
| `/xai/callback`, `/v1/ws` | xAI OAuth 回调 / WS |

## CI/CD Workflows

| Workflow | 触发 | 用途 |
|---|---|---|
| `release.yaml` | `fork/v*` tag · `workflow_dispatch` | 同步管理面板；5 平台交叉编译（darwin/linux/windows × amd64/arm64）；Conventional Commits release notes |
| `docker-image.yml` | `fork/v*` tag · `workflow_dispatch` | 同步管理面板；并行构建 linux/amd64 + linux/arm64；推送 `ghcr.io`；GHA 缓存 |
| `pr-path-guard.yml` | PR | 保护 `internal/translator/` 目录 |

---

## Fork-Specific Modifications (Upstream Merge Protection)

> **合并上游时必须保护下列行为和文件**。清单以当前 `upstream/main`（v8.0.32）为基线；TUI 已在上游，不是 fork 增量。被覆盖/删除，或重新引入下面明确删除的上游文件，均属于合并错误。

### 1. Usage Persistence & SQLite Store

Fork-only:
- `internal/usage/store.go`（+`_test.go`）— SQLite usage 日志持久化与自动清理
- `internal/usage/database_plugin.go`（+`_test.go`）— PG 日志镜像到本地 SQLite
- `internal/usage/monitor_queries.go`（+`_test.go`）— dashboard 聚合查询
- `internal/usage/logger_plugin.go`（+`_test.go`）— usage 统计 logger plugin

Modified:
- `internal/config/config.go` — `usage-persistence-enabled`
- `sdk/cliproxy/builder.go` — usage persistence builder option
- `sdk/cliproxy/service.go`（+`service_usage_persistence_test.go`）— store 生命周期 init/close

### 2. Management Center, Auth Files & Response Compression

Fork-only:
- `internal/api/handlers/management/monitor.go`、`monitor_provider_map.go`（+tests）— 13 个 monitor 端点：dashboard、provider-map、request-logs、channel-stats、failure-analysis、kpi、model-distribution、daily-trend、hourly-models、hourly-tokens、service-health、key-stats、request-details
- `internal/api/handlers/management/auth_files_list.go`（+tests）— auth 文件服务端分页、过滤与排序
- `internal/api/handlers/management/auth_files_sub2api.go` — sub2api 凭证导入
- `internal/api/handlers/management/panel.go`（+tests）— panel latest/version 与手动更新 API
- `internal/api/middleware/response_compression.go`（+tests）— `/v0` 响应压缩

Modified:
- `internal/api/handlers/management/auth_files.go`（+tests）— 大批量处理、字段批量 patch、`.txt`/zip/tar/tar.gz 上传、过滤删除、symlink/path traversal 防护、delete-all 流式处理，并通过 watcher 同步运行态
- `internal/api/handlers/management/api_tools.go` — `CleanupCodexAuth`
- `internal/api/handlers/management/usage.go` — usage export/import 增强
- `internal/api/server.go` — 注册 13 个 monitor 端点、panel API、auth load status、auth batch fields 与 `/custom/codex-cleanup`

### 3. Routing, Cooldown Persistence & Credential Lifecycle

Fork-only:
- `sdk/cliproxy/auth/routing_strategy.go`（+tests）— 路由策略常量、别名归一化 `NormalizeRoutingStrategy` 与 `SelectorForRoutingStrategy`；未知值回退 round-robin
- `sdk/cliproxy/auth/cooldown_state_persister.go` — 增量、合并批次式冷却状态持久化
- `internal/store/postgres_cooldown_store.go`（+tests）— PostgreSQL 事务型 cooldown store

Modified:
- `sdk/cliproxy/auth/selector.go`、`scheduler.go`、`conductor.go`（+tests）— 按凭证 priority 分组、单/混合 provider 调度；使用统一的 `request-retry`/`max-retry-credentials`，不得恢复 selector 级 retry override
- `sdk/cliproxy/auth/cooldown_state.go`（+tests）— 文件 cooldown store、启动恢复与按 auth 增量清理
- `sdk/cliproxy/auth/auto_refresh_loop.go`、`persist_policy.go` — 刷新失败生命周期处理；`delete-unauthorized-auth` 对终态无效凭证及 401 生效

合并保护:
- 凭证删除/替换必须串行完成并同步 token store、watcher 与调度器状态；多 provider 清理按类型执行。
- `save-cooldown-status` 关闭时不得残留运行态或持久化冷却；开启时文件/PG 后端均须在启动时恢复。

### 4. Progressive Auth Loading & Watcher

Fork-only:
- `internal/watcher/auth_load.go`、`auth_load_status.go`（+benchmark/tests）— 有界 worker pool、渐进式注册、扫描代次与加载状态
- `sdk/cliproxy/service_progressive_auth_loading_test.go` — 服务先启动、凭证后台持续加载的回归覆盖

Modified:
- `internal/config/config.go`、`config.example.yaml` — `auth-load-workers`，默认 16，限制为 1–64
- `internal/watcher/{watcher,clients,events,dispatcher,config_reload}.go`、`synthesizer/file.go` — 新扫描取代旧扫描；过期/失败扫描不得发布；文件事件与全量扫描保持一致
- `sdk/cliproxy/service.go`、`watcher.go` — 启动不等待全部 auth 文件；暴露 `/v0/management/auth-files/load-status`

### 5. Management Panel Assets

Fork-only:
- `internal/managementasset/assets/management.html.gz`、`panel-manifest.json` — 构建时内嵌的已校验基线
- `internal/managementasset/embed.go`、`manifest.go`、`replace_file_{unix,windows}.go` — manifest/SHA256 验证、兼容版本选择、跨平台原子替换
- `.github/scripts/sync-management-panel.sh` — release/docker 构建前从 fork 面板仓库同步并校验资源

Modified:
- `internal/managementasset/updater.go`（+tests）— 固定仓库 `caidaoli/Cli-Proxy-API-Management-Center`；每 3 小时检查；仅安装更新且兼容的磁盘版本
- `internal/config/config.go` — 移除用户自定义 panel repository 的实际控制权，防止运行时切换资源来源

资源选择优先级:
1. `MANAGEMENT_PANEL_DEV_PATH` 显式开发覆盖；跳过自动更新。
2. SHA256 正确、版本更新且兼容的磁盘资源。
3. 内嵌基线；磁盘资源损坏、过旧或不兼容时回退。

自动更新在开发覆盖、Home/cluster 模式、面板禁用或 `disable-auto-update` 时必须跳过。`MANAGEMENT_STATIC_PATH` 只控制磁盘升级目录，不得绕过 manifest/hash 校验。

### 6. Startup & Provider Policy

Fork-only:
- `cmd/server/config_bootstrap.go`（+tests）— Git-backed config 缺失时从模板初始化并持久化
- `cmd/server/local_model.go`（+tests）— `local-model` CLI/config 优先级；启用后禁用远程模型刷新并使用内嵌 catalog，不是本地模型目录

合并保护:
- Provider 层不保留 fork 改动，合并时直接取上游：`internal/runtime/executor/`（含 Codex / Antigravity / Claude / xAI / Kimi / Devin / OpenAI-compat）、`internal/auth/`、`internal/translator/`、`internal/registry/models/`、`cmd/fetch_*_models`；`sdk/api/handlers` 不按工具类型过滤 provider。
- 例外：`helps/utls_client.go`（第 7 节）及 `transport_decorator_test.go`；`helps/usage_helpers.go` 的 usage source 使用凭证文件名（第 1 节）；`translator/interactions/import_boundary_test.go` 的 v8 模块路径修正。
- 上游 `api_tools_test.go` 的 `TestAPICallUsesXAITokenStorageWithoutRefresh`、`TestAPICallPrioritizesStorageAccessTokenOverMetadataIDToken` 在 fork 中删除：fork 管理端 api-call 经 executor 注入凭证，不支持仅存于 Storage 的 xAI token。合并时不要恢复。

`internal/translator/` 仍是受保护路径。合并冲突必须按协议结构解析后验证，不得用字符串拼接或脆弱源码测试掩盖行为差异。

### 7. uTLS Protected-Host Transport

Modified:
- `internal/runtime/executor/helps/utls_client.go`（+tests）— protected host 自动降级：`uTLS HTTP/2 -> uTLS HTTP/1.1 -> standard HTTP/1.1`；仅在未取得 HTTP response 的 transport error 时继续，任何 HTTP status（含 401/403/429/5xx）都直接返回；POST 降级必须通过 `GetBody` 重放请求体。
  - 不得恢复 `CLIPROXY_CODEX_TRANSPORT` / `codexTransportMode` 手动开关。
  - 不得套用“所有 fallback upstream 强制 HTTP/1.1”；普通非 protected host 保留默认 HTTP/2，只有第三段 `protectedFallbackHTTP11` 使用 standard HTTP/1.1。
  - `utlsH2DegradeCache` 按 `scope+host`（scope=`direct`/`proxy:<redacted>`）缓存 response-less h2 transport error；初始 2m，指数退避封顶 30m；任意 h2 response 立即重置；注入 transport 或非 protected host 不缓存。
  - `roundTripProtected` 必须用 `sentAttempts` 而非循环下标；跳过 h2 时 HTTP/1.1 是首次实际发送，不要求 `GetBody`。

### 8. Utilities

- `internal/util/util.go` — `GetEnvTrimmed(keys...)`：按顺序读取并 trim 首个非空环境变量

### 9. CI/CD, Build & Dependencies

Modified:
- `Dockerfile` — native `GOARCH` 交叉编译（弃用 QEMU）
- `.github/workflows/docker-image.yml` — DockerHub→GHCR、matrix、GHA 缓存、`fork/v*`、`workflow_dispatch`、panel sync
- `.github/workflows/release.yaml` — `fork/v*`、`workflow_dispatch`、release artifacts、panel sync

Fork deletions（上游同步后仍必须删除）:
- `.github/workflows/pr-test-build.yml`
- `.github/workflows/auto-retarget-main-pr-to-dev.yml`

Other:
- `.gitignore` — 忽略 `usage.*`
- `go.mod`/`go.sum` — fork 增量依赖为 `modernc.org/sqlite` 及其间接依赖、`golang.org/x/exp`；Charmbracelet/TUI 依赖已属于上游

### 10. Documentation

- `README.md`/`README_CN.md` — Fork Features、Management Center、重试语义与配置说明
