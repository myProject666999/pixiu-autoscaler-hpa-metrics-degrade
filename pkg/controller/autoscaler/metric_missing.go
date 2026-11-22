/*
Copyright 2021 The Pixiu Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package autoscaler

import (
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	clock "k8s.io/utils/clock"

	"github.com/caoyingjunz/pixiu-autoscaler/pkg/controller"
)

// 指标缺失策略注解。取值 hold / reclaim，缺省（未配置或空值）等价 hold。
const (
	// OnMetricsMissingAnnotation 配置在 Deployment 上的可选降级策略注解
	OnMetricsMissingAnnotation = controller.PixiuRootPrefix + controller.PixiuSeparator + "onMetricsMissing"

	PolicyHold    = "hold"
	PolicyReclaim = "reclaim"
)

// HPA 控制器在无法获取指标时写入 ScalingActive condition 的原因。
// 这些常量在上游 k8s.io/kubernetes 的 HPA 控制器内部定义，api 包不导出，
// 此处按官方字符串集中维护，便于扩展。
const (
	reasonFailedGetResourceMetric = "FailedGetResourceMetric"
	reasonFailedGetExternalMetric = "FailedGetExternalMetric"
	reasonFailedGetPodsMetric     = "FailedGetPodsMetric"
	reasonFailedGetObjectMetric   = "FailedGetObjectMetric"
)

// 降级事件 reason，同一次中断内各自只通知一次。
const (
	eventMetricMissingObserved          = "MetricMissingObserved"
	eventMetricPartiallyMissing         = "MetricPartiallyMissing"
	eventMetricMissingGracePeriod       = "MetricMissingGracePeriod"
	eventMetricMissingReclaimed         = "MetricMissingReclaimed"
	eventMetricMissingRecovered         = "MetricMissingRecovered"
	eventMetricMissingRecreate          = "MetricMissingRecreate"
	eventMetricMissingRecreateExhausted = "MetricMissingRecreateExhausted"
	eventInvalidMetricMissingPolicy     = "InvalidMetricMissingPolicy"
)

// 时间参数（建议默认值，可调）。任务时间线中的 10 分钟宽限、10:15 重建
// 仅为测试参数，不代表这里的默认值。
const (
	metricMissingGracePeriod = 10 * time.Minute
	recreateInitialBackoff   = 30 * time.Second
	recreateMaxBackoff       = 5 * time.Minute
	maxRecreateAttempts      = 5
)

// metricFailureReasons 是可判定为“指标缺失”的 ScalingActive 原因清单。
var metricFailureReasons = map[string]controller.Empty{
	reasonFailedGetResourceMetric: {},
	reasonFailedGetExternalMetric: {},
	reasonFailedGetPodsMetric:     {},
	reasonFailedGetObjectMetric:   {},
}

// 降级监控器的内部阶段。
const (
	stageHealthy       = "Healthy"
	stageObserved      = "Observed"
	stageDegradedHold  = "DegradedHold"
	stageReclaimWait   = "ReclaimWait"
	stageReclaimGiveUp = "ReclaimGiveUp"
)

// metricMissingState 记录单个 Deployment（key = namespace/name）的一次中断状态。
// 起点始终取自 HPA 对象的 LastTransitionTime，控制器重启后内存状态丢失也会
// 依据对象上的时间重新计算，不会凭空重置起点。
type metricMissingState struct {
	stage string

	// failingSince 为当前连续中断的起点（ScalingActive=False 的 transition 时间）
	failingSince time.Time
	// lastReason 为当前中断观测到的指标失败原因
	lastReason string

	// 事件去重位，恢复后清零
	observedEventSent bool
	graceEventSent    bool
	reclaimEventSent  bool
	// 部分指标缺失（ScalingActive 仍为 True）只在同一段健康期内报告一次
	partialEventSent bool

	// reclaim 退避重建
	recreateNotBefore time.Time
	recreateAttempts  int
	// reclaimedUID 记录已回收 HPA 的 UID，用于区分“旧对象仍在缓存中等待删除”
	// 与“新 UID 对象重新出现”，避免删除事件传播延迟导致状态被提前复位。
	reclaimedUID types.UID
}

// degradeAction 告诉 sync 循环本次需要执行的动作。
type degradeAction string

const (
	actionNone     degradeAction = ""
	actionReclaim  degradeAction = "Reclaim"
	actionRecreate degradeAction = "Recreate"
)

// degradeDecision 是一次观测的结论。
type degradeDecision struct {
	action       degradeAction
	requeueAfter time.Duration
	// hpa 承载本次决策关联的 HPA（用于受控删除），可能为空
	hpa *autoscalingv2.HorizontalPodAutoscaler
}

// metricMissingMonitor 维护所有托管 Deployment 的指标缺失状态机。
type metricMissingMonitor struct {
	clock       clock.Clock
	recorder    record.EventRecorder
	states      map[string]*metricMissingState
	grace       time.Duration
	backoff0    time.Duration
	backoffMax  time.Duration
	maxAttempts int
}

func newMetricMissingMonitor(c clock.Clock, recorder record.EventRecorder) *metricMissingMonitor {
	return &metricMissingMonitor{
		clock:       c,
		recorder:    recorder,
		states:      make(map[string]*metricMissingState),
		grace:       metricMissingGracePeriod,
		backoff0:    recreateInitialBackoff,
		backoffMax:  recreateMaxBackoff,
		maxAttempts: maxRecreateAttempts,
	}
}

func (m *metricMissingMonitor) state(key string) *metricMissingState {
	if s, ok := m.states[key]; ok {
		return s
	}
	s := &metricMissingState{stage: stageHealthy}
	m.states[key] = s
	return s
}

// parsePolicy 解析注解。未配置/空值返回 hold（保持既有行为）；非法值同样
// 回退 hold，并由调用方决定是否告警。
func parsePolicy(annotations map[string]string) (policy string, invalid bool) {
	raw, ok := annotations[OnMetricsMissingAnnotation]
	if !ok || len(strings.TrimSpace(raw)) == 0 {
		return PolicyHold, false
	}
	switch raw {
	case PolicyHold, PolicyReclaim:
		return raw, false
	default:
		return PolicyHold, true
	}
}

// observe 对单个 Deployment 及其托管 HPA 做一次指标缺失状态机评估。
// 未配置降级策略时返回 no-op，保持控制器既有写入行为。
func (m *metricMissingMonitor) observe(d *appsv1.Deployment, hpaList []*autoscalingv2.HorizontalPodAutoscaler) degradeDecision {
	key, err := cache.MetaNamespaceKeyFunc(d)
	if err != nil {
		klog.ErrorS(err, "metric-missing: failed to get deployment key")
		return degradeDecision{}
	}

	policy, invalid := parsePolicy(d.GetAnnotations())
	if invalid {
		m.recorder.Eventf(d, corev1.EventTypeWarning, eventInvalidMetricMissingPolicy,
			"Invalid %s=%q on Deployment %s, fallback to %s",
			OnMetricsMissingAnnotation, d.GetAnnotations()[OnMetricsMissingAnnotation], key, PolicyHold)
	}
	// 未配置注解的部署完全保持既有行为：不建状态、不发降级事件、不重排。
	if _, configured := d.GetAnnotations()[OnMetricsMissingAnnotation]; !configured {
		if s, ok := m.states[key]; ok {
			m.resetState(s)
		}
		return degradeDecision{}
	}

	s := m.state(key)
	now := m.clock.Now()

	// reclaim 删除 HPA 后进入退避等待：此时列表为空，优先处理重建逻辑，
	// 不能被“没有 condition”路径清掉。
	if s.stage == stageReclaimWait {
		return m.reconcileReclaimWait(d, hpaList, s, now, key)
	}
	if s.stage == stageReclaimGiveUp {
		// 耗尽后停止主动重建；HPA 重新出现（例如人工或 spec 变更触发的常规
		// sync 重建）则回到健康路径重新探测。
		if len(hpaList) == 0 {
			return degradeDecision{}
		}
	}

	// 没有托管 HPA（尚未创建、已被常规流程删除等）：无 condition 可观测，
	// 清除一切中断状态。
	if len(hpaList) == 0 {
		delete(m.states, key)
		return degradeDecision{}
	}
	hpa := hpaList[0]

	cond := findScalingActiveCondition(hpa)
	if cond == nil {
		// 新对象尚无 condition：从初始状态开始，不启动计时。
		klog.V(4).InfoS("metric-missing: ScalingActive condition not present yet", "hpa", klog.KObj(hpa))
		m.resetState(s)
		return degradeDecision{}
	}

	// 时间缺失或落在未来：数据不可信，不启动计时。
	if cond.LastTransitionTime.IsZero() || cond.LastTransitionTime.Time.After(now) {
		klog.V(4).InfoS("metric-missing: ignore condition with zero/future transition time",
			"hpa", klog.KObj(hpa), "lastTransitionTime", cond.LastTransitionTime)
		m.resetState(s)
		return degradeDecision{}
	}

	switch {
	case cond.Status == corev1.ConditionTrue:
		// ScalingActive 仍为 True：即便部分指标缺失也只观察、不降级。
		// 多个 condition 共存（例如带 ScalingLimited）视为有附加观测信息。
		if len(hpa.Status.Conditions) > 1 && !s.partialEventSent {
			m.recorder.Eventf(hpa, corev1.EventTypeWarning, eventMetricPartiallyMissing,
				"HPA %s reports auxiliary conditions while ScalingActive=True; observe only, no degrade",
				klog.KObj(hpa))
			s.partialEventSent = true
		}
		return m.transitionHealthy(d, hpa, s, now)
	case cond.Status == corev1.ConditionFalse:
		if _, isMetricFailure := metricFailureReasons[cond.Reason]; !isMetricFailure {
			// 非指标类原因（正常波动、伸缩受限等）不视为缺指标。
			klog.V(4).InfoS("metric-missing: ScalingActive=False for non-metric reason, ignore",
				"hpa", klog.KObj(hpa), "reason", cond.Reason)
			return m.transitionHealthy(d, hpa, s, now)
		}
		return m.transitionFailing(d, hpa, s, cond, policy, now, key)
	default:
		// Unknown：没有有效观测，保持现状，不计时。
		klog.V(4).InfoS("metric-missing: ScalingActive=Unknown, keep current state", "hpa", klog.KObj(hpa))
		return degradeDecision{}
	}
}

// transitionHealthy 处理健康观测：恢复事件只在确实经历过中断时发出，
// 随后状态完全复位，下一次失败重新计时（不累计多次中断）。
// 已经处于健康态时保留 partialEventSent 去重位，避免部分缺失事件重复通知。
func (m *metricMissingMonitor) transitionHealthy(d *appsv1.Deployment, hpa *autoscalingv2.HorizontalPodAutoscaler,
	s *metricMissingState, now time.Time) degradeDecision {
	if s.stage == stageHealthy {
		return degradeDecision{}
	}
	duration := time.Duration(0)
	if !s.failingSince.IsZero() {
		duration = now.Sub(s.failingSince)
	}
	m.recorder.Eventf(hpa, corev1.EventTypeNormal, eventMetricMissingRecovered,
		"HPA %s ScalingActive recovered from reason=%s after %s",
		klog.KObj(hpa), s.lastReason, duration.Round(time.Second))
	m.resetState(s)
	return degradeDecision{}
}

// transitionFailing 处理一次指标类失败观测，按 hold / reclaim 推进状态机。
func (m *metricMissingMonitor) transitionFailing(d *appsv1.Deployment, hpa *autoscalingv2.HorizontalPodAutoscaler,
	s *metricMissingState, cond *autoscalingv2.HorizontalPodAutoscalerCondition,
	policy string, now time.Time, key string) degradeDecision {
	transitionAt := cond.LastTransitionTime.Time

	// 新的中断（此前健康，或原因/起点变化）：以 transition 时间重新计时。
	if s.stage == stageHealthy || !s.failingSince.Equal(transitionAt) {
		s.stage = stageObserved
		s.failingSince = transitionAt
		s.lastReason = cond.Reason
		s.observedEventSent = false
		s.graceEventSent = false
		s.reclaimEventSent = false
	}
	s.lastReason = cond.Reason

	if !s.observedEventSent {
		m.recorder.Eventf(hpa, corev1.EventTypeWarning, eventMetricMissingObserved,
			"HPA %s metric missing observed, reason=%s at %s",
			klog.KObj(hpa), cond.Reason, transitionAt.Format(time.RFC3339))
		s.observedEventSent = true
	}

	elapsed := now.Sub(transitionAt)
	if elapsed <= m.grace {
		// 未到期：等待宽限期边界再评估。
		return degradeDecision{requeueAfter: m.grace - elapsed}
	}

	// 已持续超过宽限期。
	if !s.graceEventSent {
		m.recorder.Eventf(hpa, corev1.EventTypeWarning, eventMetricMissingGracePeriod,
			"HPA %s metric missing (reason=%s) persisted %s beyond grace period %s, policy action=%s",
			klog.KObj(hpa), cond.Reason, elapsed.Round(time.Second), m.grace, policy)
		s.graceEventSent = true
	}

	if policy == PolicyHold {
		// hold：HPA 与目标副本数保持不动，只让异常可见。
		s.stage = stageDegradedHold
		return degradeDecision{}
	}

	// reclaim：只回收托管 HPA，目标保持当前副本数（不回 minReplicas）。
	if s.stage != stageReclaimWait {
		s.stage = stageReclaimWait
		s.recreateAttempts = 0
		s.recreateNotBefore = now // 删除后立即具备首次重建资格，实际退避在删除后计算
		s.reclaimedUID = hpa.UID
	}
	if !s.reclaimEventSent {
		m.recorder.Eventf(hpa, corev1.EventTypeWarning, eventMetricMissingReclaimed,
			"Reclaiming managed HPA %s: metric missing (reason=%s) persisted %s, target replicas unchanged",
			klog.KObj(hpa), cond.Reason, elapsed.Round(time.Second))
		s.reclaimEventSent = true
	}
	return degradeDecision{action: actionReclaim, hpa: hpa}
}

// reconcileReclaimWait 处理 reclaim 删除后的有界退避重建。
func (m *metricMissingMonitor) reconcileReclaimWait(d *appsv1.Deployment, hpaList []*autoscalingv2.HorizontalPodAutoscaler,
	s *metricMissingState, now time.Time, key string) degradeDecision {
	// HPA 重新出现：只有 UID 与被回收对象不同，才是真正的新对象，
	// 从初始状态重新探测；UID 相同说明删除事件尚在传播，继续等待。
	if len(hpaList) > 0 {
		if hpaList[0].UID != s.reclaimedUID {
			klog.V(2).InfoS("metric-missing: new HPA object reappeared after reclaim, restart probing from initial state",
				"hpa", klog.KObj(hpaList[0]))
			m.resetState(s)
			return degradeDecision{}
		}
		// 旧对象仍在 informer 缓存中，按等待处理（其删除会再次入队）。
		return degradeDecision{requeueAfter: m.backoff0}
	}

	if now.Before(s.recreateNotBefore) {
		return degradeDecision{requeueAfter: s.recreateNotBefore.Sub(now)}
	}
	if s.recreateAttempts >= m.maxAttempts {
		if s.stage != stageReclaimGiveUp {
			s.stage = stageReclaimGiveUp
			m.recorder.Eventf(d, corev1.EventTypeWarning, eventMetricMissingRecreateExhausted,
				"Give up recreating HPA for Deployment %s after %d failed attempts; replicas left unchanged",
				key, s.recreateAttempts)
		}
		return degradeDecision{}
	}

	s.recreateAttempts++
	backoff := m.backoffForAttempt(s.recreateAttempts)
	m.recorder.Eventf(d, corev1.EventTypeNormal, eventMetricMissingRecreate,
		"Recreating HPA for Deployment %s (attempt %d/%d) with the latest valid spec; next backoff %s",
		key, s.recreateAttempts, m.maxAttempts, backoff)
	// 下一次允许重建的时间；若本次重建成功且新 HPA 再次失败，状态机会走全新中断。
	s.recreateNotBefore = now.Add(backoff)
	return degradeDecision{action: actionRecreate, requeueAfter: backoff}
}

// backoffForAttempt 返回第 attempt 次尝试之后的退避：指数增长并封顶。
func (m *metricMissingMonitor) backoffForAttempt(attempt int) time.Duration {
	backoff := m.backoff0
	for i := 1; i < attempt; i++ {
		backoff *= 2
		if backoff >= m.backoffMax {
			return m.backoffMax
		}
	}
	if backoff > m.backoffMax {
		return m.backoffMax
	}
	return backoff
}

func (m *metricMissingMonitor) resetState(s *metricMissingState) {
	*s = metricMissingState{stage: stageHealthy}
}

// findScalingActiveCondition 返回 ScalingActive 条件，不存在返回 nil。
// 仅关注 ScalingActive；AbleToScale / ScalingLimited 的变化不参与判定。
func findScalingActiveCondition(hpa *autoscalingv2.HorizontalPodAutoscaler) *autoscalingv2.HorizontalPodAutoscalerCondition {
	for i := range hpa.Status.Conditions {
		c := &hpa.Status.Conditions[i]
		if c.Type == autoscalingv2.ScalingActive {
			return c
		}
	}
	return nil
}

// condition 是测试与状态机共用的构造辅助。
func makeHPACondition(t autoscalingv2.HorizontalPodAutoscalerConditionType, status corev1.ConditionStatus,
	reason string, at metav1.Time) autoscalingv2.HorizontalPodAutoscalerCondition {
	return autoscalingv2.HorizontalPodAutoscalerCondition{
		Type:               t,
		Status:             status,
		Reason:             reason,
		LastTransitionTime: at,
		Message:            reason,
	}
}
