"""SETUP 页配置注册表。

这是 /setup 页面树形目录的“静态元数据”来源：每个配置项的名称、分类、
详细作用说明、类型、取值范围与默认值都在这里集中维护。

运行时“当前值”与“写入/热生效”逻辑由 app.py 维护（因为它需要访问运行时
全局变量与 StateStore），本模块只做纯数据定义，避免导入时产生依赖副作用。
"""

# 分类顺序即为树形目录自上而下的顺序。
# cat: 分类 key；label: 分类名（中文）；desc: 分类的一句话说明。
CATEGORIES = [
    {"id": "override",  "label": "环境变量来源", "desc": "决定 .env 与系统环境变量谁的优先级更高。"},
    {"id": "logging",   "label": "日志与观测",   "desc": "诊断日志、重复在途请求观察与短窗结果恢复。"},
    {"id": "capture",   "label": "全文请求留存", "desc": "把发给上游的完整请求正文留存到本地 SQLite，用于排查。"},
    {"id": "ratelimit", "label": "限流与排队",   "desc": "触发上游 429 限流后，单请求允许等待的累计上限。"},
    {"id": "affinity",  "label": "会话与粘性",   "desc": "同一会话/协议回退后保持同一条 Channel 的闲置时间。"},
    {"id": "timeout",   "label": "上游超时",     "desc": "调用上游模型的响应、首包与整体活动超时控制。"},
    {"id": "hedge",     "label": "对冲竞速",     "desc": "在保证响应与兜底可靠性之间，多 Channel 并行竞速的等待节奏。"},
    {"id": "largectx",  "label": "大上下文策略", "desc": "上下文很大时采用的更激进的对冲与硬截止时间。"},
    {"id": "probe",     "label": "探活与自愈",   "desc": "对退避/冷却 Channel 进行后台健康探活的节奏。"},
]

# type: bool / int
# unit: 单位备注（秒、条、字节、MiB 等）
# hot : True=保存后立即热生效；False=保存后需重启核心 Router(:7800) 才生效。
# min/max: int 范围（bool 忽略）。default: 默认值（与代码缺省保持一致）。
ITEMS = [
    # ---------- 环境变量来源 ----------
    {"key": "FLEX_OVERRIDE", "cat": "override",
     "label": "Override（环境变量优先级）",
     "desc": "开启时，.env 文件中的同名变量会覆盖系统环境变量；关闭时系统环境变量优先、.env 的同名项被忽略。"
             "它只影响 Flex 显式命名解析的那批环境变量（各 Channel 的 API Base / API Key 等），"
             "用于在 .env 与 systemd/launchd 注入的系统变量之间快速切换生效来源。",
     "type": "bool", "min": 0, "max": 1, "unit": "", "hot": True, "default": "1"},

    # ---------- 日志与观测 ----------
    {"key": "FLEX_DEBUG", "cat": "logging",
     "label": "诊断日志（Debug）",
     "desc": "开启后记录每次请求/响应（含对上游的请求 payload 与收到的响应）供 /api/debug/recent 诊断使用；"
             "关闭则不记录。只影响诊断可观测性，不会改变任何路由行为。",
     "type": "bool", "min": 0, "max": 1, "unit": "", "hot": True, "default": "0"},
    {"key": "FLEX_DEBUG_MAX_ROWS", "cat": "logging",
     "label": "诊断日志保留条数",
     "desc": "Debug 日志在本地 SQLite 中最多保留多少个最近条目，超出部分按 FIFO 淘汰。"
             "设大有助于回溯较长时间的问题，但会占用更多磁盘。",
     "type": "int", "min": 1, "max": 100000, "unit": "条", "hot": True, "default": "1000"},
    {"key": "FLEX_DEBUG_RETENTION_DAYS", "cat": "logging",
     "label": "诊断日志保留天数",
     "desc": "Debug 日志超过多少天即被清理，与“保留条数”两者共同约束存储上限。"
             "保留越久越利于对比历史，但磁盘占用随之增加。",
     "type": "int", "min": 1, "max": 365, "unit": "天", "hot": True, "default": "7"},
    {"key": "FLEX_DUPLICATE_OBSERVER", "cat": "logging",
     "label": "重复在途请求观察器",
     "desc": "只“观察并记录”同一 Runner、同一客户端在极短时间内发出的完全相同在途请求（同指纹、仍 running 且未回包）。"
             "它不会取消、合并或延迟任何调用，也不会改动路由决策；仅把重叠情况写入 duplicate_observations 表供分析，"
             "帮助你判断客户端是否在重复重试，从而决定是否需要在业务侧去重。",
     "type": "bool", "min": 0, "max": 1, "unit": "", "hot": True, "default": "0"},
    {"key": "FLEX_RESPONSE_REPLAY_SECONDS", "cat": "logging",
     "label": "结果恢复（Replay）窗口",
     "desc": "完全相同的非流式无工具请求（同一客户端、同一模型、同一请求指纹）若成功过，在其响应仍在窗口内时直接复用上次结果、"
             "不再调用上游，用于吸收重复重试引发的上游抖动。设成 0 可关闭恢复。注意：此值在模块 import 时读取，改后需重启核心生效。",
     "type": "int", "min": 0, "max": 86400, "unit": "秒", "hot": False, "default": "120"},
    {"key": "FLEX_RESPONSE_REPLAY_MAX_BYTES", "cat": "logging",
     "label": "结果恢复单条存储上限",
     "desc": "可被恢复的成功响应正文超过该字节数则不缓存，避免超大 payload 挤占磁盘。"
             "对超大模型输出关闭恢复能显著降低写入开销。注意：此值在模块 import 时读取，改后需重启核心生效。",
     "type": "int", "min": 0, "max": 268435456, "unit": "字节", "hot": False, "default": "1048576"},

    # ---------- 全文请求留存 ----------
    {"key": "FLEX_FULL_REQUEST_CAPTURE", "cat": "capture",
     "label": "全文上行留存开关",
     "desc": "开启后会把“实际发给上游的完整请求正文”留存到本地 SQLite（不保存 Header / API Key，也不额外保存流式输出）。"
             "用于排查上游请求到底长什么样。敏感内容仅留在本机数据库，不会外发。",
     "type": "bool", "min": 0, "max": 1, "unit": "", "hot": True, "default": "0"},
    {"key": "FLEX_FULL_REQUEST_CAPTURE_HOURS", "cat": "capture",
     "label": "全文留存窗口",
     "desc": "留存超过该小时数的请求正文会被清理（即只保留最近 N 小时的全文）。"
             "该值与“最大条数”“最大空间”三个上限共同约束磁盘占用，任何一个先触发都会淘汰最旧数据。",
     "type": "int", "min": 1, "max": 24, "unit": "小时", "hot": True, "default": "3"},
    {"key": "FLEX_FULL_REQUEST_CAPTURE_MAX_ROWS", "cat": "capture",
     "label": "全文留存最大条数",
     "desc": "最多保留多少条请求全文，超出即按最旧淘汰。窗口小时数与条数/空间阈值互为兜底，防止某一维度失控。",
     "type": "int", "min": 1, "max": 10000, "unit": "条", "hot": True, "default": "300"},
    {"key": "FLEX_FULL_REQUEST_CAPTURE_MAX_MIB", "cat": "capture",
     "label": "全文留存最大空间",
     "desc": "请求全文总占用上限（MiB），超出即淘汰最旧记录。用于在保留完整性与磁盘开销之间取得平衡。",
     "type": "int", "min": 1, "max": 2048, "unit": "MiB", "hot": True, "default": "256"},

    # ---------- 限流与排队 ----------
    {"key": "FLEX_QUEUE_TPM", "cat": "ratelimit",
     "label": "TPM 限流等待上限",
     "desc": "触发上游 TPM（每分钟 Token）限流后进行指数退避重试时，单个请求允许“累计等待”的秒数上限。"
             "到达该上限仍未成功则放弃并回退/报错，避免一个请求被限流无限拖住。调大更宽容但会拉长单请求时延。",
     "type": "int", "min": 0, "max": 86400, "unit": "秒", "hot": True, "default": "60"},
    {"key": "FLEX_QUEUE_RPM", "cat": "ratelimit",
     "label": "RPM 限流等待上限",
     "desc": "触发上游 RPM（每分钟请求数）限流后进行指数退避重试时，单个请求允许“累计等待”的秒数上限。"
             "作用与 TPM 上限类似，只是针对请求数维度；两者独立分别生效。",
     "type": "int", "min": 0, "max": 86400, "unit": "秒", "hot": True, "default": "300"},

    # ---------- 会话与粘性 ----------
    {"key": "FLEX_SESSION_AFFINITY_IDLE_SECONDS", "cat": "affinity",
     "label": "普通会话粘性闲置时间",
     "desc": "同一“对话前缀”命中过的 Channel 会在该闲置时间内持续复用，保持同一对话尽量走同一家上游，"
             "以维持上下文一致性。闲置超时后新请求会重新参与路由选择。取值范围 60 秒～24 小时。",
     "type": "int", "min": 60, "max": 86400, "unit": "秒", "hot": True, "default": "3600"},
    {"key": "FLEX_PROTOCOL_AFFINITY_IDLE_SECONDS", "cat": "affinity",
     "label": "协议兼容粘性闲置时间",
     "desc": "当某 Channel 因协议不兼容（如不支持的参数/工具）回退到备用 Channel 后，备用者会被标记为该请求“协议已验证可用”，"
             "并在该闲置时间内继续保持，避免同一请求反复在不同的不兼容产物之间横跳。取值范围 60 秒～24 小时。",
     "type": "int", "min": 60, "max": 86400, "unit": "秒", "hot": True, "default": "3600"},

    # ---------- 上游超时 ----------
    {"key": "FLEX_UPSTREAM_RESPONSE_TIMEOUT", "cat": "timeout",
     "label": "上游响应对象超时",
     "desc": "从发起 LiteLLM 调用到拿到“响应对象/首字节”的最长等待。超时则把该上游任务标记为 504 并进入回退流程。"
             "它决定的是整个响应是否及时返回，而非首包。调大可容忍更慢的上游，但会拉长最终失败时的时延。",
     "type": "int", "min": 1, "max": 3600, "unit": "秒", "hot": True, "default": "180"},
    {"key": "FLEX_UPSTREAM_FIRST_CHUNK_TIMEOUT", "cat": "timeout",
     "label": "流式首包超时",
     "desc": "对于流式（SSE）响应，Router 会先缓冲原始事件，等待第一个事件（首包）到达的最长时间。"
             "首包迟迟不来视为上游粘滞，会推进 Hedge 计划或回退。对推理型模型可适当调大。",
     "type": "int", "min": 1, "max": 3600, "unit": "秒", "hot": True, "default": "180"},
    {"key": "FLEX_UPSTREAM_FIRST_ACTIVITY_TIMEOUT", "cat": "timeout",
     "label": "总活动超时（硬截止）",
     "desc": "整个请求从开始到收获任何可用结果的“硬活动截止”，且作为所有 Hedge/Deadline 计算的上限。"
             "它兜底所有更细粒度的超时，避免单个通道把请求无限拖住后仍无结果。",
     "type": "int", "min": 1, "max": 86400, "unit": "秒", "hot": True, "default": "900"},

    # ---------- 对冲竞速 ----------
    {"key": "FLEX_HEDGE_FIRST_SECONDS", "cat": "hedge",
     "label": "对冲：首个并行通道等待",
     "desc": "当路由决定“对冲”（多 Channel 并行竞速以保证响应/可靠性）时，在拉起第二个并行通道之前，"
             "对第一个通道的等待时长。设越短越激进（更早并行、更快拿到首个结果，但更耗上游配额）。",
     "type": "int", "min": 0, "max": 86400, "unit": "秒", "hot": True, "default": "360"},
    {"key": "FLEX_HEDGE_SECOND_SECONDS", "cat": "hedge",
     "label": "对冲：第二个并行通道等待",
     "desc": "前两个并行通道都尚无结果时，等待第三个（及以上）通道被拉起的时长。"
             "层级化对冲用递增的等待控制并行规模，兼顾时延与上游配额消耗。",
     "type": "int", "min": 0, "max": 86400, "unit": "秒", "hot": True, "default": "720"},

    # ---------- 大上下文策略 ----------
    {"key": "FLEX_LARGE_CONTEXT_THRESHOLD_TOKENS", "cat": "largectx",
     "label": "大上下文判定阈值",
     "desc": "当请求预估总 Token 数（prompt+输出）达到该阈值时，视为“大上下文”请求，切换到下方更激进的对冲与硬截止策略，"
             "否则走普通策略。用于区分普通对话与长文档/大批量任务。",
     "type": "int", "min": 1, "max": 100000000, "unit": "Token", "hot": True, "default": "100000"},
    {"key": "FLEX_LARGE_CONTEXT_HEDGE_FIRST_SECONDS", "cat": "largectx",
     "label": "大上下文：首个并行通道等待",
     "desc": "大上下文请求在拉第二个并行通道前对首通道的等待。大任务更看重尽早拿到结果，因此默认比普通对冲更激进。",
     "type": "int", "min": 1, "max": 86400, "unit": "秒", "hot": True, "default": "180"},
    {"key": "FLEX_LARGE_CONTEXT_HEDGE_SECOND_SECONDS", "cat": "largectx",
     "label": "大上下文：第二个并行通道等待",
     "desc": "大上下文请求在拉起第三个通道前对已有并行通道的等待，用于控制并行规模上限。",
     "type": "int", "min": 1, "max": 86400, "unit": "秒", "hot": True, "default": "360"},
    {"key": "FLEX_LARGE_CONTEXT_DEADLINE_ONE_SECONDS", "cat": "largectx",
     "label": "大上下文：单通道硬截止",
     "desc": "只有 1 条备选 Channel 可用时，大上下文请求的最长活动时间；超时未果即放弃并报错。",
     "type": "int", "min": 1, "max": 86400, "unit": "秒", "hot": True, "default": "360"},
    {"key": "FLEX_LARGE_CONTEXT_DEADLINE_TWO_SECONDS", "cat": "largectx",
     "label": "大上下文：双通道硬截止",
     "desc": "有 2 条备选 Channel 时，大上下文请求的最长活动时间；期间会在各通道间回退/竞速，超时则报错。",
     "type": "int", "min": 1, "max": 86400, "unit": "秒", "hot": True, "default": "360"},
    {"key": "FLEX_LARGE_CONTEXT_DEADLINE_THREE_SECONDS", "cat": "largectx",
     "label": "大上下文：三通道以上硬截止",
     "desc": "有 3 条及以上备选 Channel 时给出的最宽松硬截止，给最充分的回退/竞速空间。"
             "（另有总活动上限 UPSTREAM_FIRST_ACTIVITY_TIMEOUT 作为其封顶。）",
     "type": "int", "min": 1, "max": 86400, "unit": "秒", "hot": True, "default": "540"},

    # ---------- 探活与自愈 ----------
    {"key": "FLEX_PROBE_INTERVAL", "cat": "probe",
     "label": "后台探活间隔",
     "desc": "处于退避/冷却/疑似故障状态的 Channel 由后台任务以该间隔周期性地发起健康探活，验证其是否已恢复。"
             "设小恢复更快但会产生更多探活开销；设大则恢复滞后但配额占用低。",
     "type": "int", "min": 10, "max": 86400, "unit": "秒", "hot": True, "default": "120"},
    {"key": "FLEX_PROBE_COOLDOWN", "cat": "probe",
     "label": "探活失败冷却",
     "desc": "一次探活失败后，对该 Channel 在该冷却时间内不再重复探活，避免反复把已验证故障的通道打爆。"
             "（5 小时配额耗尽类故障采用更长的固定 1800 秒冷却。）",
     "type": "int", "min": 10, "max": 86400, "unit": "秒", "hot": True, "default": "120"},
]

# 由 key 快速索引
_BY_KEY = {it["key"]: it for it in ITEMS}


def item_for(key):
    return _BY_KEY.get(key)


def default_for(key):
    it = _BY_KEY.get(key)
    return it["default"] if it else None