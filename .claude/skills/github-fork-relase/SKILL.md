---
name: github-fork-relase
description: Use when releasing the CLIProxyAPI fork, calculating its next semantic version, creating or rolling back fork/v* tags, or triggering its GitHub Actions release workflow.
---

# Release Command

CPA 项目发布：提交→推送Tag→GitHub Actions自动编译发布

## ⚠️ 强制规则

**所有 Tag 必须使用 `fork/v*` 前缀**（避免与上游冲突）

- 正确：`fork/v6.7.0`、`fork/v1.0.0`
- 错误：`v6.7.0`、`v1.0.0`

## 回滚命令

```bash
# 推送失败时
git tag -d fork/vX.Y.Z

# 撤回发布
git push origin :refs/tags/fork/vX.Y.Z && git tag -d fork/vX.Y.Z
```

## 流程

### 1. 环境检测

```bash
git status --porcelain
```

有未提交文件时：`git add . && git commit -m "type: message"`

提交类型：`feat` `fix` `perf` `refactor` `docs` `test` `chore` `style` `build` `ci`

### 2. 版本计算

```bash
# 必须使用 --match "fork/v*" 过滤前缀
PREV_TAG=$(git describe --tags --match "fork/v*" --abbrev=0 2>/dev/null || echo "fork/v0.0.0")
COMMIT_COUNT=$(git rev-list --count --no-merges "${PREV_TAG}..HEAD")
git log "${PREV_TAG}..HEAD" --no-merges --pretty=format:'%H%n%s%n%b%n---'
```

`COMMIT_COUNT` 为 `0` 时停止：没有可发布的提交。Merge 提交标题不参与判断；使用其包含的非 Merge 提交。

**自动判断规则（按优先级）**：

| 优先级 | 条件 | 版本 |
|--------|------|------|
| 1 | 用户明确要求升级大版本，或待发布提交包含明确的 breaking 标记：标题 `type!:` / `type(scope)!:`，或正文 footer `BREAKING CHANGE:` / `BREAKING-CHANGE:` | major |
| 2 | `feat:`，或提交语义明确表示新增、引入、支持、启用对外能力 | minor |
| 3 | 修复、性能、重构、文档、测试、依赖、构建、CI、内部实现，以及无法识别的提交 | patch |

判断约束：

- 用户明确指定版本或要求维持当前大版本时，以用户要求为准。
- 扫描全部待发布的非 Merge 提交，按上述条件取最高等级：`major > minor > patch`。breaking 标记必须是提交自身的声明，引用示例或否定说明不算。
- 禁止根据 diff 或提交语义推断 `major`。上游合并、输入校验收紧、错误处理、行为修复及配置优先级调整，没有明确 breaking 标记时均不触发大版本升级；实际兼容性影响写入发布说明。
- 只有新增对外能力才能推断为 `minor`；新增测试、文档、内部 helper 仍为 `patch`
- 不符合 Conventional Commits 且语义不明确时，默认 `patch`，不询问用户

版本计算：

| 等级 | 计算 |
|------|------|
| major | `X.Y.Z → (X+1).0.0` |
| minor | `X.Y.Z → X.(Y+1).0` |
| patch | `X.Y.Z → X.Y.(Z+1)` |

设置 `NEW_TAG=fork/v<计算后的版本>`，输出上一个 Tag、判断等级、决定性提交和 `NEW_TAG`，然后直接继续发布，不要求用户选择版本。

### 3. 创建并推送Tag

```bash
# 已存在时停止，禁止覆盖 Tag
git rev-parse -q --verify "refs/tags/${NEW_TAG}"

# NEW_TAG 格式必须是 fork/vX.Y.Z
git tag -a "${NEW_TAG}" -m "Release ${NEW_TAG#fork/}"
git push origin "$(git branch --show-current)" "${NEW_TAG}"
```

`git rev-parse` 成功表示 Tag 已存在，立即停止；失败才执行创建和推送。

推送成功后 GitHub Actions 自动构建发布。
