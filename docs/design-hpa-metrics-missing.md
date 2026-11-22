# HPA 指标缺失降级策略设计（onMetricsMissing）

## 1. 背景与现状确认

`AutoscalerController.syncAutoscalers` 经 `updateHPA` 入队后，核心逻辑 `sync()`
只做规格层面的同步：

- `pkg/controller/autoscaler/autoscaler_controller.go:393` 仅以
  `reflect.DeepEqual(oldHPA.Spec, newHPA.Spec)` 判断是否需要更新 HPA；
- 全流程没有任何代码读取 `hpa.Status.Conditions`。

离线用例 `TestSyncIgnoresStatusConditions` 固化该事实：仅修改
`status.conditions`（不改动 spec）时，sync 不产生任何 HPA 写操作。
本设计在不改变该默认行为的前提下，新增**显式可选**的指标缺失处理。

## 2. 注解与策略

Deployment 注解（沿用 `hpa.caoyingjunz.io/` 前缀）：

| 注解 | 取值 | 默认 | 说明 |
| --- | --- | --- | --- |
| `hpa.caoyingjunz.io/onMetricsMissing` | `hold` / `reclaim` | `hold` | 指标持续缺失时的动作 |

- 未配置该注解、或值为空：等价 `hold`，且**完全保持既有写入行为**（不新增任何
  HPA/Deployment 写操作），仅在真正进入异常时记录事件。
- 非法值：按 `hold` 处理并记录 Warning 事件，不阻塞正常 sync。
- 每次 sync 从最新 Deployment 注解重新解析策略；策略在运行中切换时按新策略执行。

### hold（默认）

宽限期内不动作；超过宽限期后**不删除、不修改** HPA 与目标副本数，仅发出一次
`MetricMissingDegraded` 事件使持续异常可见。ScalingActive 恢复 True 后发出一次
`MetricMissingRecovered` 事件并清除计时。

### reclaim

超过宽限期后**仅删除本控制器托管的 HPA**（owner 为该 Deployment 且由
`getHPAsForDeployment` 选出），目标 Deployment 的 `spec.replicas` 保持当前值，
**绝不回退到 minReplicas**，也不向 Deployment 写入任何状态注解。删除后按
有界退避尝试重建（见第 5 节）。

## 3. 判定规则

判定对象是 HPA 条件中的 `ScalingActive`：

1. 取 `Type == ScalingActive` 的 condition；不存在则视为**无有效观测**，
   不启动计时（新对象尚无 condition 的正常路径）。
2. 仅当 `Status == False` 且 `Reason` 属于指标获取失败类原因时才视为一次失败：
   - `FailedGetResourceMetric`
   - `FailedGetExternalMetric`
   - `FailedGetPodsMetric`
   - `FailedGetObjectMetric`
   - （指标类原因清单集中定义在 `metric_missing.go`，可扩展）
3. 起点取该 condition 的 `LastTransitionTime`：
   - 缺失（零值）：不启动计时；
   - 时间在未来（晚于可替换时钟的 Now）：视为异常数据，不启动计时；
   - 控制器重启**不重置起点**——起点来自 HPA 对象自身的 transition 时间，
     内存状态丢失后重新 sync 时按对象上的时间重新计算，不凭空延长或缩短。
4. 持续时间 `clock.Since(LastTransitionTime) > 宽限期` 才触发动作。
5. 下列情况**不得**当作持续缺指标：
   - 新建 HPA 无任何 condition；
   - condition 时间缺失或落在未来；
   - 只有 `AbleToScale` / `ScalingLimited` 变化（不看这两个 condition）；
   - `ScalingActive` 仍为 `True`，即使部分指标缺失（HPA 会在 message 中体现）：
     只报告观察事件，不自动降级；
   - `ScalingActive=False` 但原因不在指标类清单内（例如 `DesiredWithinTolerance`
     之类的正常原因）。
6. **恢复重新计时**：condition 恢复为 True 后再次失败，以新的
   `LastTransitionTime` 重新计时，不累计此前的中断时长。

## 4. 状态机与事件

内存中按 Deployment key（`namespace/name`）维护 `metricMissingState`，
所有时间判断走 `clock.Clock` 接口（生产 `clock.RealClock`，测试
`clocktesting.FakeClock`）。状态字段：失败起点、是否已发宽限事件、是否已发降级
事件、reclaim 阶段（退避截止时间、已尝试次数）。

| 时机 | 事件 reason | 类型 | 内容 |
| --- | --- | --- | --- |
| 首次观测到指标类失败（进入观测） | `MetricMissingObserved` | Normal/Warning | HPA 名称、原因 |
| 持续时间达到宽限期 | `MetricMissingGracePeriod` | Warning | HPA 名称、原因、持续时长、动作(hold/reclaim) |
| reclaim 实际删除 HPA | `MetricMissingReclaimed` | Warning | HPA 名称、原因、持续时长、动作=reclaim |
| 恢复为 ScalingActive=True | `MetricMissingRecovered` | Normal | HPA 名称、本次中断持续时长 |
| 退避重建 HPA | `MetricMissingRecreate` | Normal | HPA 名称、第几次尝试 |
| 重建尝试耗尽 | `MetricMissingRecreateExhausted` | Warning | HPA 名称、最大次数 |
| 部分缺失但 ScalingActive=True | `MetricPartiallyMissing` | Warning | 仅观察，不降级 |

同一次中断内每种事件只发一次（用状态中的布尔位去重）；恢复后清除布尔位，
下次中断重新通知。事件消息统一包含 HPA 名称、原因、持续时间、动作。

## 5. HPA 删除后的恢复探测（reclaim 专属）

reclaim 删除 HPA 后无法再观察其 condition，方案为**有界退避重建**：

- 删除后进入 `reclaimWaiting`，记录退避截止时间与尝试次数；
- 每次该 Deployment 再入队 sync 时，若时钟已过截止时间且未超过最大次数，
  使用**最新 Deployment 注解重新生成的有效规格**（`CreateHPAFromDeployment`）
  重建 HPA。重建不使用任何缓存快照，因此回收期间的注解变更不会被旧快照覆盖；
- 新对象没有 condition，状态机回到初始状态重新探测；若新 HPA 再次持续失败，
  按全新中断重新计时（不累计）；
- 重建失败（API 错误）按既有 workqueue 限速重试，不消耗退避次数；
- 达到最大次数仍无法稳定存在：发 `MetricMissingRecreateExhausted`，停止重建，
  保留 Deployment 当前副本数；Deployment 注解再次变化会重新入队并可重新评估。

### 建议默认值（均为可调常量，待最终确认）

| 参数 | 建议默认 |
| --- | --- |
| 宽限期 `metricMissingGracePeriod` | 10 分钟 |
| 初始退避 `recreateInitialBackoff` | 30 秒 |
| 退避上限 `recreateMaxBackoff` | 5 分钟 |
| 最大重建次数 `maxRecreateAttempts` | 5 |

退避序列：`min(初始退避 * 2^(attempt-1), 退避上限)`，有界。
任务时间线（10:00 失败、宽限 10 分钟、10:15 重建）仅为测试参数，不代表默认值。

## 6. 适配器副作用与稳定性契约

自定义指标 HPA 带 `hpa.caoyingjunz.io/targetCustomMetric=true` 标签，其删除会让
`syncConfigMaps` 重算 externalRules 并经 `notifyAdapter` 重启 prometheus-adapter；
重建又会触发一次。若退避过短，适配器会随降级探测反复重启。

稳定性契约（本设计只承诺契约，不重写规则生成器）：

1. reclaim 删除/重建 HPA 后，`syncConfigMaps` 仅在 externalRules **实际变化**时
   写 ConfigMap 并通知适配器——现有 `reflect.DeepEqual(cfg.ExternalRules,
   externalRules)` 短路已提供该保证，降级逻辑不绕过它；
2. 退避有界且有最大次数，适配器重启次数因此有上界（≤ 2 × maxRecreateAttempts）；
3. hold 策略不删除 HPA，对适配器零副作用；
4. “保留规则不摘除”等进一步取舍属待确认项，不在本次实现范围内。

## 7. 接入原 sync 的方式

`AutoscalerController` 新增字段：`clock clock.Clock`、`degrade *metricMissingMonitor`。
`NewAutoscalerController` 中初始化为 `clock.RealClock{}`（保留一个包内构造函数
允许测试注入 fake clock 与 fake recorder）。

`syncAutoscalers` 在取到 Deployment 与现有 HPA 列表后、进入原 `sync()` 前调用
`ac.observeMetricMissing(d, hpaList)`，返回观察结论与可能的动作：

- hold 且未到期：原 sync 全量执行（spec 比较、更新逻辑不变）；
- hold 已到期：原 sync 照常执行（保持 HPA 不动），只多发事件；
- reclaim 已到期且 HPA 仍存在：执行受控删除（仅删除 owner 匹配的托管 HPA，
  外来 HPA 不可删除），随后进入退避，本次跳过常规 spec 同步；
- reclaim 等待退避：到期则用最新规格重建，未到期则跳过 HPA 写操作；
- 未配置注解：监控器直接返回 no-op，行为与今天完全一致。

监控器通过 `queue.AddAfter(key, requeueAfter)` 安排下次评估（等待宽限期/退避），
workqueue 对相同 key 天然去重；HPA condition 每次变化也会经 `updateHPA`
重新入队，两条路径互补。

## 8. 测试方案（全部离线）

手工构造 HPA Conditions，配合 `clocktesting.NewFakeClock` 与
`fake.NewSimpleClientset`，事件用 `record.NewFakeRecorder` 断言。用例：

1. 正常（ScalingActive=True）无事件、无写操作；
2. 未到期（失败持续 < 宽限期）不动作；
3. hold 超时：只发一次事件，HPA 仍存在、副本数不变；
4. reclaim 超时：托管 HPA 被删除，Deployment replicas 不变、不回 minReplicas；
5. 回收重建：退避到期后用最新规格重建，新对象从初始状态重新探测；
6. 抖动：失败→恢复 True→再失败，以新 transition 时间重新计时，10:05 恢复则
   10:10 不得沿用 10:00 起点触发；
7. 新对象无 condition：不启动计时；
8. 部分指标缺失但 ScalingActive=True：只报告不降级；
9. 未来时间 / 缺失时间：不启动计时；
10. 外来 HPA（owner 不匹配）reclaim 也不可删除；
11. 仅有 AbleToScale/ScalingLimited 变化：不触发；
12. 退避耗尽：发 exhausted 事件并停止重建；
13. 未配置注解：零降级写操作（回归既有行为）；
14. 控制器“重启”（新建监控器，对象保留旧 transition 时间）：起点不被重置；
15. `sync` 忽略 status.Conditions 的基线事实用例。
