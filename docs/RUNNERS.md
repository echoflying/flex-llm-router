# Runner 统一资源模型

Flex 对外统一暴露 **Runner**。Runner 是一个稳定的外部模型名，可以包含一个或多个内部 Channel；调度策略沿用原 Pool 的 `selection`、`tiers`、会话粘性和 Hedge 配置。

一个 Channel 始终是一个 Provider + Model 对。单 Channel Runner 和多 Channel Runner 使用同一套结构，修改成员数量不会改变 Runner 的外部名称、Base URL 或 URL。

```yaml
runners:
  coder:
    public_model: coder
    channels: [deepseek-sensenova]
    tiers: {deepseek-sensenova: 0}
    selection:
      strategy: round_robin

links:                         # 旧版 connections 的兼容别名
  hermes-coder: coder
```

旧配置中的 `pools` 会在加载时迁移为 `runners`，旧 `connections` 会迁移为 `links`；旧 API 地址继续保留。新 UI/API 只把 Runner 作为主要资源展示，旧字段仅用于兼容。

Provider 下拉和 Model 候选接口默认只读取已配置的 Provider/Channel；用户点击 Model 测试/刷新时，才会用该 Provider 的凭据显式请求一次 `/models`，不执行周期性主动健康检查。Channel 状态仅在加入 Runner、真实流量或上游故障时更新。

## Config 三标签页

`/config` 固定提供三个标签页，顺序为 **Runner → Channel → Model**：

- **Runner** 编辑对外模型名、成员 Channel 和成员顺序；策略字段仍复用既有 Pool 策略。
- **Channel** 编辑 Provider/`litellm_model`，以及 `enabled` 和 `externally_exposed`。关闭 `externally_exposed` 只隐藏该 Channel 的直接外部模型目录项，不会将它从 Runner 内部路由移除。
- **Model** 管理 Provider，并只写入 `base_url_env` / `api_key_env` 这类 `.env` 变量名引用；实际密钥值永远不进入 API 响应或页面。

页面保存通过 `/api/config/runners/{name}`、`/api/config/channels/{id}` 和
`/api/config/providers` 完成，服务端先执行完整 `FlexConfig` 校验，再备份并热更新内存配置；不会自动重启核心。
