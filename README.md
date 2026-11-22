# Pixiu Autoscaler

`pixiu-autoscaler` 通过为 `workload` 添加 `annotations` 的方式，自动维护对应 `HorizontalPodAutoscaler` 的生命周期.

## Prerequisites

在 `kubernetes` 集群中， 需要先完成 `Metrics Server` 组件的安装，请参考 [Metrics Server](https://github.com/kubernetes-incubator/metrics-server)

`kubectl top node/pod` 验证 `Metrics Server` 已成功安装

``` bash
# kubectl top node
NAME          CPU(cores)   CPU%   MEMORY(bytes)   MEMORY%
kubez         333m         16%    1225Mi          65%

# kubectl top pod
NAME                     CPU(cores)   MEMORY(bytes)
test1-54cd855b77-q67h6   1m           3Mi
```

## Installing

The steps can be found in [Installation](./deploy)

## Getting Started

在 `workload` 的 `annotations` 中添加所需注释即可自动创建对应的 `HPA`

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  annotations:
    ...
    # 可选，默认 minReplicas 为 1
    hpa.caoyingjunz.io/minReplicas: "2"  # MINPODS
    # 可选，默认 maxReplicas 为 6
    hpa.caoyingjunz.io/maxReplicas: "6"  # MAXPODS
    ...

    # 支持多种 TARGETS 类型，若开启，至少选择一种，可同时支持多个 TARGETS
    # CPU, in cores. (500m = .5 cores)
    # Memory, in bytes. (500Gi = 500GiB = 500 * 1024 * 1024 * 1024)

    # 使用率 examples
    cpu.hpa.caoyingjunz.io/targetAverageUtilization: "80"
    memory.hpa.caoyingjunz.io/targetAverageUtilization: "60"

    # 使用值 examples
    cpu.hpa.caoyingjunz.io/targetAverageValue: 600m
    memory.hpa.caoyingjunz.io/targetAverageValue: 60Mi

    # prometheus examples
    # TODO
    ...
  name: test1
  namespace: default
  ...
```

`pixiu-autoscaler-controller` 会根据注释的变化，自动同步 `HPA` 的生命周期.

## 指标缺失降级策略（可选）

当 HPA 持续无法获取指标（`ScalingActive=False` 且原因为
`FailedGetResourceMetric` / `FailedGetExternalMetric` 等）时，可通过 Deployment
注解配置降级行为：

```yaml
annotations:
  # 可选：hold（默认）或 reclaim
  hpa.caoyingjunz.io/onMetricsMissing: hold
```

- `hold`（默认）：超过宽限期（默认 10 分钟）后保持 HPA 与目标副本数不变，
  仅通过事件（`MetricMissingGracePeriod` 等）使持续异常可见。
- `reclaim`：超过宽限期后删除本控制器托管的 HPA，目标 Deployment 保持当前
  副本数（不回退到 minReplicas），随后按有界指数退避（默认最多 5 次）用最新
  规格重建探测；重建尝试耗尽会发出 `MetricMissingRecreateExhausted` 事件。
- 未配置该注解的 Deployment 行为与历史版本完全一致，不产生任何降级写操作。
- 判定只依据 `ScalingActive` 条件及其 `LastTransitionTime`；新对象无
  condition、时间缺失/落在未来、仅 `AbleToScale`/`ScalingLimited` 变化、
  或 `ScalingActive` 仍为 `True`（部分指标缺失）均不会触发降级；恢复 `True`
  后再次失败会重新计时。

详细设计见 [docs/design-hpa-metrics-missing.md](./docs/design-hpa-metrics-missing.md)。


Copyright 2019 caoyingjunz (cao.yingjunz@gmail.com) Apache License 2.0
