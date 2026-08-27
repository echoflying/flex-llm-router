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

- **Runner** 以横向表格行展示，每个 Runner 独占一行；左侧为模型名、策略和地址信息，右侧为按顺序纵向排列的 Channel。模型名默认只读，悬停后可点“编辑”；Channel 可上移、下移、移除或通过独立弹窗增加成员，新增项排在末尾，顺序就是调度/Hedge 的候选顺序，所有操作确认后立即保存，不再需要额外保存按钮。移除操作保持显示，最后一个 Channel 不允许移除。策略显示中文名称，`i` 图标查看说明，“更换”弹窗直接列出 Radio 选项切换既有策略；增加 Channel 弹窗按 Provider 分组并把 Provider 放在选项前面。页面在 Runner 区域提供 Base URL 本机/局域网选择和“增加 Runner”入口，并可复制 Base URL 与对外模型名。策略字段仍复用既有 Pool 策略。
- **Channel** 管理 Provider/`litellm_model` 和 `enabled`。Channel 始终是 Runner 的内部成员，不在 `/v1/models` 中单独展示；旧配置中的 `externally_exposed` 字段仍保留用于兼容，但不再在 Channel 页面单独配置。
- Channel 还保存 `protocol_support` 的显式检测结果。管理员可在 Config → Channel 点击“检测 Responses”，结果会记录为 `supported`、`unsupported` 或 `error`，并显示最近检测时间/原因；不会周期性主动探测。
- Runner 页可直接“增加 Runner”：输入名称并选择首个 Channel，创建后立即生效；名称仅允许字母、数字、点、下划线和连字符，首字符必须为字母或数字且最多 64 个字符，不支持空格和斜杠；给单 Channel Runner 增加第二个 Channel 时，弹窗会要求先选择策略。
- 每个 Channel 行提供“移除”；移除后立即保存，且最后一个 Channel 不允许移除。
- Channel 添加流程先选择 Provider，再显式加载上游模型并勾选一个或多个；系统按 `provider-model` 生成默认 Channel ID，别名写入 `public_model`。已有 Channel 的 Provider/Model 对固定，编辑只修改别名、暴露和启用状态。
- Channel 页按 Provider 分组（Provider 单元格纵向合并，不插入额外分隔行），可通过“添加”选择 Provider 与 Model 创建新 Channel；每行显示最后访问时间，并提供“自检 | 编辑”操作。自检仅在点击时发起一次真实调用。
- **Model** 管理 Provider，并只写入 `base_url_env` / `api_key_env` 这类 `.env` 变量名引用；实际密钥值永远不进入 API 响应或页面。

页面保存通过 `/api/config/runners/{name}`、`/api/config/channels/{id}` 和
`/api/config/providers` 完成，服务端先执行完整 `FlexConfig` 校验，再备份并热更新内存配置；不会自动重启核心。
