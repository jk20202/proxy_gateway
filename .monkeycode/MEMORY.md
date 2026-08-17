# User Instruction Memory

This file records user instructions, preferences, and teachings for reference in future interactions.

## Format

### User Instruction Entry
User instruction entries should follow this format:

[User Instruction Summary]
- Date: [YYYY-MM-DD]
- Context: [Mentioned scenario or time]
- Instructions:
  - [Content of user teaching or instruction, described line by line]

### Project Knowledge Entry
Entries discovered by the Agent during task execution should follow this format:

[Project Knowledge Summary]
- Date: [YYYY-MM-DD]
- Context: Discovered by Agent while performing [specific task description]
- Category: [Operations & Deployment|Build Methods|Testing Methods|Troubleshooting & Debugging|Workflow & Collaboration|Environment Configuration]
- Instructions:
  - [Specific knowledge points, described line by line]

## Deduplication Strategy
- Before adding a new entry, check for similar or identical instructions.
- If a duplicate is found, skip the new entry or merge it with the existing one.
- When merging, update the context or date information.
- This helps avoid redundant entries and keeps the memory file tidy.

## Entries

[Project Knowledge Summary]
- Date: 2026-08-14
- Context: Discovered by Agent while performing 跨端重构与前端 UI 改动验证
- Category: Build Methods
- Instructions:
  - 前端在 `internal/server/web/index.html` 单文件内，经 `go:embed` 嵌入；改动 JS/CSS 后必须 `go build -o proxy-pool ./cmd/proxy-pool` 重编译并重启服务才会生效，否则页面仍是旧版本。
  - 服务以 `./proxy-pool -config config.yaml` 启动，监听 8080（web/API）与 10000（代理网关）。
  - jsdom 前端回归脚本位于 `/tmp/opencode/regress.js`（`node /tmp/opencode/regress.js`）；JS 语法校验：`sed -n '/<script>/,/<\/script>/p' internal/server/web/index.html | sed '1d;$d' > /tmp/opencode/index.js && node --check /tmp/opencode/index.js`。
  - 后端质量门槛：`go vet ./...` + `go test ./...`；提交前需 `gofmt -w`（gofmt -l 必须为空）。
  - 管理端登录：`POST /api/v1/auth/login` body `{"name":"admin","password":"admin123"}`；admin token 可在 config.yaml 配置。
