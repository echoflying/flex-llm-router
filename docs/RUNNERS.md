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

Provider 下拉和 Model 候选接口只读取已配置的 Provider/Channel，不执行周期性主动健康检查。Channel 状态仅在加入 Runner、真实流量或上游故障时更新。
