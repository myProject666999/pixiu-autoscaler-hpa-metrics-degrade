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
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
)

const (
	testNamespace = "default"
	testDeploy    = "demo"
	testHPA       = "demo-hpa"
)

var testStart = time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC)

func newTestDeployment(policy string) *appsv1.Deployment {
	annotations := map[string]string{
		"cpu." + "hpa.caoyingjunz.io" + "/targetAverageUtilization": "80",
		"hpa.caoyingjunz.io/maxReplicas":                            "6",
	}
	if policy != "" {
		annotations[OnMetricsMissingAnnotation] = policy
	}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:        testDeploy,
			Namespace:   testNamespace,
			UID:         "deploy-uid-1",
			Annotations: annotations,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32Ptr(3),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "demo"}},
		},
	}
}

func newTestHPA(owner *appsv1.Deployment, conditions ...autoscalingv2.HorizontalPodAutoscalerCondition) *autoscalingv2.HorizontalPodAutoscaler {
	controller := true
	return &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testHPA,
			Namespace: testNamespace,
			UID:       "hpa-uid-1",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       owner.Name,
				UID:        owner.UID,
				Controller: &controller,
			}},
		},
		Status: autoscalingv2.HorizontalPodAutoscalerStatus{Conditions: conditions},
	}
}

func newForeignHPA(conditions ...autoscalingv2.HorizontalPodAutoscalerCondition) *autoscalingv2.HorizontalPodAutoscaler {
	controller := true
	return &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foreign-hpa",
			Namespace: testNamespace,
			UID:       "hpa-uid-foreign",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       "someone-else",
				UID:        "other-uid",
				Controller: &controller,
			}},
		},
		Status: autoscalingv2.HorizontalPodAutoscalerStatus{Conditions: conditions},
	}
}

func scalingActive(status corev1.ConditionStatus, reason string, at time.Time) autoscalingv2.HorizontalPodAutoscalerCondition {
	return makeHPACondition(autoscalingv2.ScalingActive, status, reason, metav1.NewTime(at))
}

func newMonitorWithClock(t *testing.T) (*metricMissingMonitor, *clocktesting.FakeClock, *record.FakeRecorder) {
	t.Helper()
	fc := clocktesting.NewFakeClock(testStart)
	rec := record.NewFakeRecorder(64)
	m := newMetricMissingMonitor(fc, rec)
	return m, fc, rec
}

func drainEvents(rec *record.FakeRecorder) []string {
	var events []string
	for {
		select {
		case e := <-rec.Events:
			events = append(events, e)
		default:
			return events
		}
	}
}

func eventReasons(events []string) []string {
	reasons := make([]string, 0, len(events))
	for _, e := range events {
		// FakeRecorder 事件格式: "EventType EventReason Message"
		fields := strings.Fields(e)
		if len(fields) < 2 {
			reasons = append(reasons, e)
			continue
		}
		reasons = append(reasons, fields[1])
	}
	return reasons
}

func containsReason(reasons []string, want string) bool {
	for _, r := range reasons {
		if r == want {
			return true
		}
	}
	return false
}

func int32Ptr(i int32) *int32 { return &i }

// TestParsePolicy 验证策略解析：缺省/空=hold，非法回退 hold 并标记。
func TestParsePolicy(t *testing.T) {
	cases := []struct {
		name        string
		annotations map[string]string
		wantPolicy  string
		wantInvalid bool
	}{
		{"missing", map[string]string{}, PolicyHold, false},
		{"empty", map[string]string{OnMetricsMissingAnnotation: "  "}, PolicyHold, false},
		{"hold", map[string]string{OnMetricsMissingAnnotation: "hold"}, PolicyHold, false},
		{"reclaim", map[string]string{OnMetricsMissingAnnotation: "reclaim"}, PolicyReclaim, false},
		{"invalid", map[string]string{OnMetricsMissingAnnotation: "delete-it-all"}, PolicyHold, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, invalid := parsePolicy(tc.annotations)
			if got != tc.wantPolicy || invalid != tc.wantInvalid {
				t.Fatalf("parsePolicy() = (%q, %v), want (%q, %v)", got, invalid, tc.wantPolicy, tc.wantInvalid)
			}
		})
	}
}

// TestHealthyNoAction ScalingActive=True 时无动作、无事件。
func TestHealthyNoAction(t *testing.T) {
	m, _, rec := newMonitorWithClock(t)
	d := newTestDeployment(PolicyReclaim)
	hpa := newTestHPA(d, scalingActive(corev1.ConditionTrue, "ValidMetricFound", testStart))

	got := m.observe(d, []*autoscalingv2.HorizontalPodAutoscaler{hpa})
	if got.action != actionNone || got.requeueAfter != 0 {
		t.Fatalf("expected no decision, got %+v", got)
	}
	if evs := drainEvents(rec); len(evs) != 0 {
		t.Fatalf("expected no events, got %v", evs)
	}
}

// TestNoConditionDoesNotStartTimer 新对象没有 ScalingActive condition：不计时。
func TestNoConditionDoesNotStartTimer(t *testing.T) {
	m, fc, rec := newMonitorWithClock(t)
	d := newTestDeployment(PolicyReclaim)
	hpa := newTestHPA(d) // 无 condition

	for i := 0; i < 5; i++ {
		fc.Step(3 * time.Minute)
		got := m.observe(d, []*autoscalingv2.HorizontalPodAutoscaler{hpa})
		if got.action != actionNone {
			t.Fatalf("step %d: expected no action without condition, got %+v", i, got)
		}
	}
	if evs := drainEvents(rec); len(evs) != 0 {
		t.Fatalf("expected no events without condition, got %v", evs)
	}
}

// TestWithinGracePeriodNoAction 失败但宽限期未到：不触发动作。
func TestWithinGracePeriodNoAction(t *testing.T) {
	m, fc, _ := newMonitorWithClock(t)
	d := newTestDeployment(PolicyReclaim)
	hpa := newTestHPA(d, scalingActive(corev1.ConditionFalse, reasonFailedGetResourceMetric, testStart))

	got := m.observe(d, []*autoscalingv2.HorizontalPodAutoscaler{hpa})
	if got.action != actionNone {
		t.Fatalf("expected no action within grace, got %+v", got)
	}
	if got.requeueAfter != m.grace {
		t.Fatalf("expected requeue after grace %s, got %s", m.grace, got.requeueAfter)
	}

	fc.Step(5 * time.Minute) // 10:05，仅过去 5 分钟
	got = m.observe(d, []*autoscalingv2.HorizontalPodAutoscaler{hpa})
	if got.action != actionNone {
		t.Fatalf("expected no action before grace deadline, got %+v", got)
	}
}

// TestHoldTimeoutOnlyReports hold 超时：只发事件，不产生 reclaim 动作。
func TestHoldTimeoutOnlyReports(t *testing.T) {
	m, fc, rec := newMonitorWithClock(t)
	d := newTestDeployment(PolicyHold)
	hpa := newTestHPA(d, scalingActive(corev1.ConditionFalse, reasonFailedGetExternalMetric, testStart))

	m.observe(d, []*autoscalingv2.HorizontalPodAutoscaler{hpa})
	fc.Step(m.grace + time.Second) // 10:10:01
	got := m.observe(d, []*autoscalingv2.HorizontalPodAutoscaler{hpa})
	if got.action != actionNone {
		t.Fatalf("hold must never reclaim/recreate, got action %q", got.action)
	}
	reasons := eventReasons(drainEvents(rec))
	if !containsReason(reasons, eventMetricMissingGracePeriod) {
		t.Fatalf("expected grace event, got %v", reasons)
	}
	// 再 sync 一次：同一次中断不重复通知。
	fc.Step(time.Minute)
	m.observe(d, []*autoscalingv2.HorizontalPodAutoscaler{hpa})
	reasons2 := eventReasons(drainEvents(rec))
	if len(reasons2) != 0 {
		t.Fatalf("expected no duplicate events within one outage, got %v", reasons2)
	}
}

// TestReclaimTimeoutDeletes reclaim 超时：返回 Reclaim 决策（删除由控制器执行）。
func TestReclaimTimeoutReclaimDecision(t *testing.T) {
	m, fc, rec := newMonitorWithClock(t)
	d := newTestDeployment(PolicyReclaim)
	hpa := newTestHPA(d, scalingActive(corev1.ConditionFalse, reasonFailedGetResourceMetric, testStart))

	m.observe(d, []*autoscalingv2.HorizontalPodAutoscaler{hpa})
	fc.Step(m.grace + time.Second)
	got := m.observe(d, []*autoscalingv2.HorizontalPodAutoscaler{hpa})
	if got.action != actionReclaim || got.hpa == nil || got.hpa.Name != testHPA {
		t.Fatalf("expected reclaim decision for managed HPA, got %+v", got)
	}
	reasons := eventReasons(drainEvents(rec))
	if !containsReason(reasons, eventMetricMissingReclaimed) {
		t.Fatalf("expected reclaim event, got %v", reasons)
	}
}

// TestReclaimBackoffRecreate 删除后退避重建：未到期等待，到期返回 Recreate，
// 次数有界，耗尽后停止。
func TestReclaimBackoffRecreate(t *testing.T) {
	m, fc, rec := newMonitorWithClock(t)
	d := newTestDeployment(PolicyReclaim)
	hpa := newTestHPA(d, scalingActive(corev1.ConditionFalse, reasonFailedGetExternalMetric, testStart))

	// 进入 reclaim。
	m.observe(d, []*autoscalingv2.HorizontalPodAutoscaler{hpa})
	fc.Step(m.grace + time.Second)
	decision := m.observe(d, []*autoscalingv2.HorizontalPodAutoscaler{hpa})
	if decision.action != actionReclaim {
		t.Fatalf("expected reclaim, got %+v", decision)
	}

	// 模拟 HPA 已删除：立即首次重建（recreateNotBefore 初始化为删除时刻）。
	first := m.observe(d, nil)
	if first.action != actionRecreate {
		t.Fatalf("expected first recreate immediately after reclaim, got %+v", first)
	}
	if first.requeueAfter != m.backoff0 {
		t.Fatalf("expected first backoff %s, got %s", m.backoff0, first.requeueAfter)
	}

	// 退避未到期：继续等待。
	waiting := m.observe(d, nil)
	if waiting.action != actionNone || waiting.requeueAfter != m.backoff0 {
		t.Fatalf("expected to wait for backoff, got %+v", waiting)
	}

	// 每次重建后 HPA 仍缺失（模拟新对象再次失败被回收/或始终建不起来），
	// 走到最大次数后耗尽。
	var recreateCount = 1
	for {
		fc.Step(m.backoffMax + time.Second)
		next := m.observe(d, nil)
		if next.action == actionRecreate {
			recreateCount++
			continue
		}
		// 耗尽：无动作、无重排。
		if next.action != actionNone || next.requeueAfter != 0 {
			t.Fatalf("expected give-up decision, got %+v", next)
		}
		break
	}
	if recreateCount != m.maxAttempts {
		t.Fatalf("expected %d recreate attempts, got %d", m.maxAttempts, recreateCount)
	}
	reasons := eventReasons(drainEvents(rec))
	if !containsReason(reasons, eventMetricMissingRecreateExhausted) {
		t.Fatalf("expected exhausted event, got %v", reasons)
	}
	// 耗尽之后再观测也不会重建。
	fc.Step(time.Hour)
	if got := m.observe(d, nil); got.action != actionNone {
		t.Fatalf("expected no recreate after exhaustion, got %+v", got)
	}
}

// TestBackoffIsBoundedAndDoubling 验证指数退避与封顶。
func TestBackoffIsBoundedAndDoubling(t *testing.T) {
	m, _, _ := newMonitorWithClock(t)
	if got := m.backoffForAttempt(1); got != m.backoff0 {
		t.Fatalf("attempt 1 backoff = %s, want %s", got, m.backoff0)
	}
	if got := m.backoffForAttempt(2); got != 2*m.backoff0 {
		t.Fatalf("attempt 2 backoff = %s, want %s", got, 2*m.backoff0)
	}
	if got := m.backoffForAttempt(99); got != m.backoffMax {
		t.Fatalf("backoff must be capped at %s, got %s", m.backoffMax, got)
	}
}

// TestFlapRestartsTimer 抖动：10:05 恢复 True，10:10 再次失败不得沿用 10:00 起点。
func TestFlapRestartsTimer(t *testing.T) {
	m, fc, rec := newMonitorWithClock(t)
	d := newTestDeployment(PolicyReclaim)
	hpa := newTestHPA(d, scalingActive(corev1.ConditionFalse, reasonFailedGetResourceMetric, testStart))

	m.observe(d, []*autoscalingv2.HorizontalPodAutoscaler{hpa})
	fc.Step(5 * time.Minute) // 10:05 恢复
	hpa.Status.Conditions = []autoscalingv2.HorizontalPodAutoscalerCondition{
		scalingActive(corev1.ConditionTrue, "ValidMetricFound", fc.Now()),
	}
	got := m.observe(d, []*autoscalingv2.HorizontalPodAutoscaler{hpa})
	if got.action != actionNone {
		t.Fatalf("recovery should produce no action, got %+v", got)
	}
	if !containsReason(eventReasons(drainEvents(rec)), eventMetricMissingRecovered) {
		t.Fatalf("expected recovered event")
	}

	// 10:10 再次失败，transition 时间为 10:10。
	fc.Step(5 * time.Minute)
	secondFailure := fc.Now()
	hpa.Status.Conditions = []autoscalingv2.HorizontalPodAutoscalerCondition{
		scalingActive(corev1.ConditionFalse, reasonFailedGetResourceMetric, secondFailure),
	}
	m.observe(d, []*autoscalingv2.HorizontalPodAutoscaler{hpa})
	// 此刻距新起点 0 秒，绝不能因累计旧中断而立刻 reclaim。
	if got := m.observe(d, []*autoscalingv2.HorizontalPodAutoscaler{hpa}); got.action != actionNone {
		t.Fatalf("new outage must restart timing, got %+v", got)
	}
	// 再走 9 分钟（累计墙钟 10:19，但新中断只持续 9 分钟）仍未到期。
	fc.Step(9 * time.Minute)
	if got := m.observe(d, []*autoscalingv2.HorizontalPodAutoscaler{hpa}); got.action != actionNone {
		t.Fatalf("outage of 9 minutes must not trigger, got %+v", got)
	}
	// 新中断满 10 分钟才触发。
	fc.Step(time.Minute + time.Second)
	if got := m.observe(d, []*autoscalingv2.HorizontalPodAutoscaler{hpa}); got.action != actionReclaim {
		t.Fatalf("expected reclaim only after fresh grace period, got %+v", got)
	}
}

// TestFutureAndMissingTransitionTime 未来时间/零值时间不得启动计时。
func TestFutureAndMissingTransitionTime(t *testing.T) {
	m, fc, rec := newMonitorWithClock(t)
	d := newTestDeployment(PolicyReclaim)

	future := newTestHPA(d, scalingActive(corev1.ConditionFalse, reasonFailedGetResourceMetric, testStart.Add(time.Hour)))
	fc.Step(30 * time.Minute)
	if got := m.observe(d, []*autoscalingv2.HorizontalPodAutoscaler{future}); got.action != actionNone {
		t.Fatalf("future transition time must not trigger, got %+v", got)
	}

	zero := newTestHPA(d, makeHPACondition(autoscalingv2.ScalingActive, corev1.ConditionFalse,
		reasonFailedGetResourceMetric, metav1.Time{}))
	if got := m.observe(d, []*autoscalingv2.HorizontalPodAutoscaler{zero}); got.action != actionNone {
		t.Fatalf("zero transition time must not trigger, got %+v", got)
	}
	if evs := drainEvents(rec); len(evs) != 0 {
		t.Fatalf("expected no events for invalid times, got %v", evs)
	}
}

// TestNonMetricReasonsIgnored ScalingActive=False 但原因不属于指标类：忽略。
func TestNonMetricReasonsIgnored(t *testing.T) {
	m, fc, _ := newMonitorWithClock(t)
	d := newTestDeployment(PolicyReclaim)
	hpa := newTestHPA(d, scalingActive(corev1.ConditionFalse, "DesiredWithinTolerance", testStart))
	fc.Step(time.Hour)
	got := m.observe(d, []*autoscalingv2.HorizontalPodAutoscaler{hpa})
	if got.action != actionNone {
		t.Fatalf("non-metric reason must not trigger degrade, got %+v", got)
	}
}

// TestOnlyAbleToScaleChanges 仅 AbleToScale/ScalingLimited 变化：不触发。
func TestOnlyAbleToScaleChanges(t *testing.T) {
	m, fc, rec := newMonitorWithClock(t)
	d := newTestDeployment(PolicyReclaim)
	hpa := newTestHPA(d,
		makeHPACondition(autoscalingv2.AbleToScale, corev1.ConditionFalse, "FailedUpdateScale", metav1.NewTime(testStart)),
		makeHPACondition(autoscalingv2.ScalingLimited, corev1.ConditionTrue, "TooManyReplicas", metav1.NewTime(testStart)),
	)
	fc.Step(time.Hour)
	if got := m.observe(d, []*autoscalingv2.HorizontalPodAutoscaler{hpa}); got.action != actionNone {
		t.Fatalf("AbleToScale/ScalingLimited only must not trigger, got %+v", got)
	}
	if evs := drainEvents(rec); len(evs) != 0 {
		t.Fatalf("expected no events without ScalingActive condition, got %v", evs)
	}
}

// TestPartialMissingWhileActive ScalingActive=True 且伴随其他 condition：只报告不降级。
func TestPartialMissingWhileActive(t *testing.T) {
	m, fc, rec := newMonitorWithClock(t)
	d := newTestDeployment(PolicyReclaim)
	hpa := newTestHPA(d,
		scalingActive(corev1.ConditionTrue, "ValidMetricFound", testStart),
		makeHPACondition(autoscalingv2.ScalingLimited, corev1.ConditionTrue, "TooManyReplicas", metav1.NewTime(testStart)),
	)
	fc.Step(time.Hour)
	if got := m.observe(d, []*autoscalingv2.HorizontalPodAutoscaler{hpa}); got.action != actionNone {
		t.Fatalf("partial missing with ScalingActive=True must not degrade, got %+v", got)
	}
	reasons := eventReasons(drainEvents(rec))
	if !containsReason(reasons, eventMetricPartiallyMissing) {
		t.Fatalf("expected partial-missing observation event, got %v", reasons)
	}
	// 同一段健康期只报告一次。
	m.observe(d, []*autoscalingv2.HorizontalPodAutoscaler{hpa})
	if evs := drainEvents(rec); len(evs) != 0 {
		t.Fatalf("partial event must not repeat, got %v", evs)
	}
}

// TestRestartDoesNotResetStart 控制器“重启”=新建监控器，对象保留旧 transition 时间：
// 起点来自对象本身，不会凭空重置。
func TestRestartDoesNotResetStart(t *testing.T) {
	fc := clocktesting.NewFakeClock(testStart)
	rec := record.NewFakeRecorder(64)
	d := newTestDeployment(PolicyReclaim)
	hpa := newTestHPA(d, scalingActive(corev1.ConditionFalse, reasonFailedGetResourceMetric, testStart))

	m1 := newMetricMissingMonitor(fc, rec)
	m1.observe(d, []*autoscalingv2.HorizontalPodAutoscaler{hpa})

	// 重启：新监控器，无内存状态；时钟走到宽限期之后。
	fc.Step(m1.grace + time.Second)
	m2 := newMetricMissingMonitor(fc, rec)
	got := m2.observe(d, []*autoscalingv2.HorizontalPodAutoscaler{hpa})
	if got.action != actionReclaim {
		t.Fatalf("restart must not reset outage start; expected reclaim, got %+v", got)
	}
}

// TestRecreatedHPAStartsFresh 退避重建后新对象无 condition：从初始状态重新探测。
func TestRecreatedHPAStartsFresh(t *testing.T) {
	m, fc, _ := newMonitorWithClock(t)
	d := newTestDeployment(PolicyReclaim)
	hpa := newTestHPA(d, scalingActive(corev1.ConditionFalse, reasonFailedGetExternalMetric, testStart))

	m.observe(d, []*autoscalingv2.HorizontalPodAutoscaler{hpa})
	fc.Step(m.grace + time.Second)
	if got := m.observe(d, []*autoscalingv2.HorizontalPodAutoscaler{hpa}); got.action != actionReclaim {
		t.Fatalf("expected reclaim")
	}
	// 退避到期，触发重建。
	fc.Step(m.backoff0 + time.Second)
	if got := m.observe(d, nil); got.action != actionRecreate {
		t.Fatalf("expected recreate decision, got %+v", got)
	}
	// 新对象出现（新 UID）且没有 condition：状态必须已复位，再久也不会沿旧中断触发。
	fresh := newTestHPA(d)
	fresh.UID = "hpa-uid-recreated-2"
	fc.Step(time.Hour)
	if got := m.observe(d, []*autoscalingv2.HorizontalPodAutoscaler{fresh}); got.action != actionNone {
		t.Fatalf("fresh HPA without conditions must start from initial state, got %+v", got)
	}
}

// TestNoAnnotationKeepsLegacyBehavior 未配置注解：监控器 no-op。
func TestNoAnnotationKeepsLegacyBehavior(t *testing.T) {
	m, fc, rec := newMonitorWithClock(t)
	d := newTestDeployment("")
	hpa := newTestHPA(d, scalingActive(corev1.ConditionFalse, reasonFailedGetResourceMetric, testStart))
	fc.Step(2 * m.grace)
	got := m.observe(d, []*autoscalingv2.HorizontalPodAutoscaler{hpa})
	if got.action != actionNone || got.requeueAfter != 0 {
		t.Fatalf("unconfigured deployment must be a no-op, got %+v", got)
	}
	if evs := drainEvents(rec); len(evs) != 0 {
		t.Fatalf("unconfigured deployment must emit no degrade events, got %v", evs)
	}
}

// TestInvalidPolicyFallsBackToHold 非法策略按 hold 处理并发告警事件。
func TestInvalidPolicyFallsBackToHold(t *testing.T) {
	m, fc, rec := newMonitorWithClock(t)
	d := newTestDeployment("burn-it-down")
	hpa := newTestHPA(d, scalingActive(corev1.ConditionFalse, reasonFailedGetResourceMetric, testStart))
	fc.Step(m.grace + time.Second)
	got := m.observe(d, []*autoscalingv2.HorizontalPodAutoscaler{hpa})
	if got.action != actionNone {
		t.Fatalf("invalid policy must fall back to hold, got %+v", got)
	}
	reasons := eventReasons(drainEvents(rec))
	if !containsReason(reasons, eventInvalidMetricMissingPolicy) {
		t.Fatalf("expected invalid-policy event, got %v", reasons)
	}
	if !containsReason(reasons, eventMetricMissingGracePeriod) {
		t.Fatalf("hold fallback should still report grace event, got %v", reasons)
	}
}
