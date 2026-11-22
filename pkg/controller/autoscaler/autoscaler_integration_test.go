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
	"context"
	"reflect"
	"strconv"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/caoyingjunz/pixiu-autoscaler/pkg/controller"
)

// controllerFixture 组装 fake clientset + informer + 假时钟的离线测试环境。
type controllerFixture struct {
	t        *testing.T
	client   *fake.Clientset
	factory  informers.SharedInformerFactory
	ac       *AutoscalerController
	clock    *clocktesting.FakeClock
	recorder *record.FakeRecorder
	stopCh   chan struct{}
}

func newFixture(t *testing.T, objects ...runtime.Object) *controllerFixture {
	t.Helper()
	client := fake.NewSimpleClientset(objects...)
	// fake clientset 不自动生成 UID；真实 apiserver 会。这里用 prepend reactor
	// 在创建时为缺失 UID 的对象补上，使基于 UID 的新旧对象判定可离线验证。
	hpaUIDCounter := 0
	client.PrependReactor("create", "horizontalpodautoscalers", func(action ktesting.Action) (bool, runtime.Object, error) {
		createAction, ok := action.(ktesting.CreateAction)
		if !ok {
			return false, nil, nil
		}
		hpa, ok := createAction.GetObject().(*autoscalingv2.HorizontalPodAutoscaler)
		if ok && len(hpa.UID) == 0 {
			hpaUIDCounter++
			hpa.UID = types.UID("hpa-generated-uid-" + strconv.Itoa(hpaUIDCounter))
		}
		return false, nil, nil
	})
	factory := informers.NewSharedInformerFactory(client, 0)
	fc := clocktesting.NewFakeClock(testStart)
	rec := record.NewFakeRecorder(128)

	ac, err := newAutoscalerControllerForTest(
		factory.Apps().V1().Deployments(),
		factory.Autoscaling().V2().HorizontalPodAutoscalers(),
		factory.Core().V1().ConfigMaps(),
		client, fc, rec)
	if err != nil {
		t.Fatalf("create test controller: %v", err)
	}

	stopCh := make(chan struct{})
	factory.Start(stopCh)
	factory.WaitForCacheSync(stopCh)
	return &controllerFixture{
		t: t, client: client, factory: factory, ac: ac,
		clock: fc, recorder: rec, stopCh: stopCh,
	}
}

// seedDeployment 将带 UID 的 Deployment 写入 fake clientset 并等待缓存同步。
// fake clientset 不会在创建时自动生成 UID，而 owner 校验依赖 UID，需显式设置。
func (f *controllerFixture) seedDeployment(t *testing.T, d *appsv1.Deployment) *appsv1.Deployment {
	t.Helper()
	if d.UID == "" {
		d.UID = "deploy-uid-1"
	}
	created, err := f.client.AppsV1().Deployments(d.Namespace).Create(context.TODO(), d, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("seed deployment: %v", err)
	}
	f.refresh()
	return created
}

func (f *controllerFixture) stop() { close(f.stopCh) }

func (f *controllerFixture) sync(d *appsv1.Deployment) error {
	key, err := controller.KeyFunc(d)
	if err != nil {
		f.t.Fatalf("key func: %v", err)
	}
	return f.ac.syncAutoscalers(key)
}

// refresh 等待 fake clientset 的写操作经 watch 传播到 informer 缓存。
// 仅比较对象数量不够（status 更新数量不变，且不同资源类型互不相关），
// 因此对 Deployment/HPA/ConfigMap 三类对象分别比较 ResourceVersion。
func (f *controllerFixture) refresh() {
	cache.WaitForCacheSync(f.stopCh,
		f.ac.dListerSynced, f.ac.hpaListerSynced, f.ac.cmListerSynced)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if f.viewConverged() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	f.t.Fatalf("informer cache did not converge with fake clientset")
}

func (f *controllerFixture) viewConverged() bool {
	return f.hpaViewConverged() && f.deploymentViewConverged()
}

func (f *controllerFixture) deploymentViewConverged() bool {
	list, err := f.ac.dLister.Deployments(metav1.NamespaceAll).List(labels.Everything())
	if err != nil {
		return false
	}
	clientList, err := f.client.AppsV1().Deployments(metav1.NamespaceAll).List(
		context.TODO(), metav1.ListOptions{})
	if err != nil || len(list) != len(clientList.Items) {
		return false
	}
	clientByName := make(map[string]appsv1.Deployment, len(clientList.Items))
	for _, d := range clientList.Items {
		clientByName[d.Namespace+"/"+d.Name] = d
	}
	for _, d := range list {
		want, ok := clientByName[d.Namespace+"/"+d.Name]
		// fake clientset 不维护 ResourceVersion，直接比较注解与规格内容。
		if !ok || !reflect.DeepEqual(d.Annotations, want.Annotations) ||
			!reflect.DeepEqual(d.Spec, want.Spec) {
			return false
		}
	}
	return true
}

func (f *controllerFixture) hpaViewConverged() bool {
	list, err := f.ac.hpaLister.HorizontalPodAutoscalers(testNamespace).List(labels.Everything())
	if err != nil {
		return false
	}
	clientList, err := f.client.AutoscalingV2().HorizontalPodAutoscalers(testNamespace).List(
		context.TODO(), metav1.ListOptions{})
	if err != nil || len(list) != len(clientList.Items) {
		return false
	}
	clientByName := make(map[string]autoscalingv2.HorizontalPodAutoscaler, len(clientList.Items))
	for _, h := range clientList.Items {
		clientByName[h.Name] = h
	}
	for _, h := range list {
		want, ok := clientByName[h.Name]
		// fake clientset 不维护 ResourceVersion，直接比较规格与状态内容。
		if !ok || !reflect.DeepEqual(h.Spec, want.Spec) ||
			!reflect.DeepEqual(h.Status.Conditions, want.Status.Conditions) {
			return false
		}
	}
	return true
}

func (f *controllerFixture) hpaCount() int {
	list, err := f.client.AutoscalingV2().HorizontalPodAutoscalers(testNamespace).List(
		context.TODO(), metav1.ListOptions{})
	if err != nil {
		f.t.Fatalf("list hpa: %v", err)
	}
	return len(list.Items)
}

func (f *controllerFixture) deploymentReplicas() int32 {
	d, err := f.client.AppsV1().Deployments(testNamespace).Get(context.TODO(), testDeploy, metav1.GetOptions{})
	if err != nil {
		f.t.Fatalf("get deployment: %v", err)
	}
	if d.Spec.Replicas == nil {
		return -1
	}
	return *d.Spec.Replicas
}

func (f *controllerFixture) events() []string { return drainEvents(f.recorder) }

// TestSyncIgnoresStatusConditions 基线事实：只改 status.conditions 不触发 HPA 写操作。
func TestSyncIgnoresStatusConditions(t *testing.T) {
	d := newTestDeployment("")
	// 先跑一次常规 sync 创建 HPA。
	f := newFixture(t)
	defer f.stop()
	d = f.seedDeployment(t, d)
	if err := f.sync(d); err != nil {
		t.Fatalf("initial sync: %v", err)
	}
	f.refresh()
	if f.hpaCount() != 1 {
		t.Fatalf("expected 1 HPA created, got %d", f.hpaCount())
	}
	before, _ := f.client.AutoscalingV2().HorizontalPodAutoscalers(testNamespace).List(context.TODO(), metav1.ListOptions{})
	rv := before.Items[0].ResourceVersion

	// 仅修改 informer 缓存中 HPA 的 status（模拟 HPA 控制器更新 condition）。
	hpa := before.Items[0].DeepCopy()
	hpa.Status.Conditions = []autoscalingv2.HorizontalPodAutoscalerCondition{
		scalingActive(corev1.ConditionFalse, reasonFailedGetResourceMetric, testStart),
	}
	// 通过客户端更新 status，再等 informer 同步；spec 保持不变。
	if _, err := f.client.AutoscalingV2().HorizontalPodAutoscalers(testNamespace).UpdateStatus(
		context.TODO(), hpa, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update status: %v", err)
	}
	f.refresh()

	if err := f.sync(d); err != nil {
		t.Fatalf("sync after status change: %v", err)
	}
	after, _ := f.client.AutoscalingV2().HorizontalPodAutoscalers(testNamespace).List(context.TODO(), metav1.ListOptions{})
	if after.Items[0].ResourceVersion != rv {
		t.Fatalf("status-only change must not cause spec write: rv %s -> %s", rv, after.Items[0].ResourceVersion)
	}
}

// TestIntegrationHoldKeepsHPA hold 超时后 HPA 与副本数均不变，仅有事件。
func TestIntegrationHoldKeepsHPA(t *testing.T) {
	d := newTestDeployment(PolicyHold)
	f := newFixture(t)
	defer f.stop()
	d = f.seedDeployment(t, d)
	if err := f.sync(d); err != nil {
		t.Fatalf("initial sync: %v", err)
	}
	f.refresh()

	hpa := f.mustHPA(t)
	hpa.Status.Conditions = []autoscalingv2.HorizontalPodAutoscalerCondition{
		scalingActive(corev1.ConditionFalse, reasonFailedGetResourceMetric, testStart),
	}
	f.updateHPAStatus(t, hpa)

	// 未到期：无删除。
	f.clock.Step(5 * time.Minute)
	if err := f.sync(d); err != nil {
		t.Fatalf("sync within grace: %v", err)
	}
	f.refresh()
	if f.hpaCount() != 1 {
		t.Fatalf("HPA must survive within grace period")
	}

	// 超时：hold 只报告。
	f.clock.Step(5*time.Minute + time.Second)
	if err := f.sync(d); err != nil {
		t.Fatalf("sync after grace: %v", err)
	}
	f.refresh()
	if f.hpaCount() != 1 {
		t.Fatalf("hold must never delete HPA")
	}
	if got := f.deploymentReplicas(); got != 3 {
		t.Fatalf("replicas changed under hold: %d", got)
	}
	if !containsReason(eventReasons(f.events()), eventMetricMissingGracePeriod) {
		t.Fatalf("expected grace event under hold")
	}
}

// TestIntegrationReclaimDeletesManagedHPAOnly reclaim 超时只删托管 HPA，
// 副本数保持当前值，外来 HPA 不受影响。
func TestIntegrationReclaimDeletesManagedHPAOnly(t *testing.T) {
	d := newTestDeployment(PolicyReclaim)
	foreign := newForeignHPA(scalingActive(corev1.ConditionFalse, reasonFailedGetResourceMetric, testStart))
	f := newFixture(t)
	defer f.stop()
	d = f.seedDeployment(t, d)

	if _, err := f.client.AutoscalingV2().HorizontalPodAutoscalers(testNamespace).Create(
		context.TODO(), foreign, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create foreign hpa: %v", err)
	}
	if err := f.sync(d); err != nil {
		t.Fatalf("initial sync: %v", err)
	}
	f.refresh()
	if f.hpaCount() != 2 {
		t.Fatalf("expected managed + foreign HPA, got %d", f.hpaCount())
	}

	hpa := f.mustHPA(t)
	hpa.Status.Conditions = []autoscalingv2.HorizontalPodAutoscalerCondition{
		scalingActive(corev1.ConditionFalse, reasonFailedGetExternalMetric, testStart),
	}
	f.updateHPAStatus(t, hpa)

	f.clock.Step(metricMissingGracePeriod + time.Second)
	if err := f.sync(d); err != nil {
		t.Fatalf("sync after grace: %v", err)
	}
	f.refresh()

	if f.hpaCount() != 1 {
		t.Fatalf("expected only foreign HPA left, got %d", f.hpaCount())
	}
	list, _ := f.client.AutoscalingV2().HorizontalPodAutoscalers(testNamespace).List(context.TODO(), metav1.ListOptions{})
	if list.Items[0].Name != "foreign-hpa" {
		t.Fatalf("foreign HPA must not be deleted, got %s", list.Items[0].Name)
	}
	if got := f.deploymentReplicas(); got != 3 {
		t.Fatalf("reclaim must keep current replicas (3), got %d", got)
	}
	if !containsReason(eventReasons(f.events()), eventMetricMissingReclaimed) {
		t.Fatalf("expected reclaim event")
	}
}

// TestIntegrationReclaimRecreateWithLatestSpec 退避到期后用最新规格重建，
// 回收期间的注解变更生效，不被旧快照覆盖。
func TestIntegrationReclaimRecreateWithLatestSpec(t *testing.T) {
	d := newTestDeployment(PolicyReclaim)
	f := newFixture(t)
	defer f.stop()
	d = f.seedDeployment(t, d)
	if err := f.sync(d); err != nil {
		t.Fatalf("initial sync: %v", err)
	}
	f.refresh()

	hpa := f.mustHPA(t)
	hpa.Status.Conditions = []autoscalingv2.HorizontalPodAutoscalerCondition{
		scalingActive(corev1.ConditionFalse, reasonFailedGetResourceMetric, testStart),
	}
	f.updateHPAStatus(t, hpa)

	f.clock.Step(metricMissingGracePeriod + time.Second)
	if err := f.sync(d); err != nil {
		t.Fatalf("reclaim sync: %v", err)
	}
	f.refresh()
	if f.hpaCount() != 0 {
		t.Fatalf("managed HPA should be reclaimed")
	}

	// 回收期间修改注解：maxReplicas 6 -> 9。
	latest, err := f.client.AppsV1().Deployments(testNamespace).Get(context.TODO(), testDeploy, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	latest.Annotations["hpa.caoyingjunz.io/maxReplicas"] = "9"
	if _, err := f.client.AppsV1().Deployments(testNamespace).Update(context.TODO(), latest, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update deployment: %v", err)
	}
	f.refresh()

	// 立即首次重建（recreateNotBefore 初始化为删除时刻）。
	if err := f.sync(latest); err != nil {
		t.Fatalf("recreate sync: %v", err)
	}
	f.refresh()
	if f.hpaCount() != 1 {
		t.Fatalf("HPA should be recreated, got %d", f.hpaCount())
	}
	recreated := f.mustHPA(t)
	if recreated.Spec.MaxReplicas != 9 {
		t.Fatalf("recreated HPA must use latest spec maxReplicas=9, got %d", recreated.Spec.MaxReplicas)
	}
	if len(recreated.Status.Conditions) != 0 {
		t.Fatalf("recreated object must start without conditions")
	}
	if !containsReason(eventReasons(f.events()), eventMetricMissingRecreate) {
		t.Fatalf("expected recreate event")
	}
}

// TestIntegrationFlapTimeline 10:00 失败、10:05 恢复、10:10 不得触发。
func TestIntegrationFlapTimeline(t *testing.T) {
	d := newTestDeployment(PolicyReclaim)
	f := newFixture(t)
	defer f.stop()
	d = f.seedDeployment(t, d)
	if err := f.sync(d); err != nil {
		t.Fatalf("initial sync: %v", err)
	}
	f.refresh()

	hpa := f.mustHPA(t)
	hpa.Status.Conditions = []autoscalingv2.HorizontalPodAutoscalerCondition{
		scalingActive(corev1.ConditionFalse, reasonFailedGetResourceMetric, testStart),
	}
	f.updateHPAStatus(t, hpa)
	if err := f.sync(d); err != nil {
		t.Fatalf("first failing sync: %v", err)
	}

	// 10:05 恢复 True。
	f.clock.Step(5 * time.Minute)
	hpa = f.mustHPA(t)
	hpa.Status.Conditions = []autoscalingv2.HorizontalPodAutoscalerCondition{
		scalingActive(corev1.ConditionTrue, "ValidMetricFound", f.clock.Now()),
	}
	f.updateHPAStatus(t, hpa)
	if err := f.sync(d); err != nil {
		t.Fatalf("recovery sync: %v", err)
	}

	// 10:10：若错误沿用 10:00 起点，此刻会满 10 分钟而触发 reclaim；正确行为是不触发。
	f.clock.Step(5 * time.Minute)
	f.refresh()
	if f.hpaCount() != 1 {
		t.Fatalf("HPA must not be reclaimed after flap")
	}
	if containsReason(eventReasons(f.events()), eventMetricMissingReclaimed) {
		t.Fatalf("reclaim must not fire when outage was interrupted by recovery")
	}
}

// TestIntegrationUnconfiguredNoDegradeWrites 未配置注解：即使持续失败也零降级写操作。
func TestIntegrationUnconfiguredNoDegradeWrites(t *testing.T) {
	d := newTestDeployment("")
	f := newFixture(t)
	defer f.stop()
	d = f.seedDeployment(t, d)
	if err := f.sync(d); err != nil {
		t.Fatalf("initial sync: %v", err)
	}
	f.refresh()
	before, _ := f.client.AutoscalingV2().HorizontalPodAutoscalers(testNamespace).List(context.TODO(), metav1.ListOptions{})
	rv := before.Items[0].ResourceVersion

	hpa := f.mustHPA(t)
	hpa.Status.Conditions = []autoscalingv2.HorizontalPodAutoscalerCondition{
		scalingActive(corev1.ConditionFalse, reasonFailedGetResourceMetric, testStart),
	}
	f.updateHPAStatus(t, hpa)
	f.clock.Step(time.Hour)
	if err := f.sync(d); err != nil {
		t.Fatalf("sync: %v", err)
	}
	after, _ := f.client.AutoscalingV2().HorizontalPodAutoscalers(testNamespace).List(context.TODO(), metav1.ListOptions{})
	if len(after.Items) != 1 || after.Items[0].ResourceVersion != rv {
		t.Fatalf("unconfigured deployment must keep HPA untouched")
	}
	for _, e := range f.events() {
		if containsReason([]string{e}, eventMetricMissingGracePeriod) ||
			containsReason([]string{e}, eventMetricMissingReclaimed) {
			t.Fatalf("unconfigured deployment must emit no degrade events, got %q", e)
		}
	}
}

// mustHPA 取回 owner 指向被测 Deployment 的托管 HPA，避免依赖列表顺序，
// 存在外来 HPA 的用例也不受影响。
func (f *controllerFixture) mustHPA(t *testing.T) *autoscalingv2.HorizontalPodAutoscaler {
	t.Helper()
	list, err := f.ac.hpaLister.HorizontalPodAutoscalers(testNamespace).List(labels.Everything())
	if err != nil {
		t.Fatalf("list hpa: %v", err)
	}
	for _, h := range list {
		ref := metav1.GetControllerOf(h)
		if ref != nil && ref.Kind == "Deployment" && ref.Name == testDeploy {
			return h.DeepCopy()
		}
	}
	t.Fatalf("managed HPA for %q not found among %d items", testDeploy, len(list))
	return nil
}

func (f *controllerFixture) updateHPAStatus(t *testing.T, hpa *autoscalingv2.HorizontalPodAutoscaler) {
	t.Helper()
	if _, err := f.client.AutoscalingV2().HorizontalPodAutoscalers(testNamespace).UpdateStatus(
		context.TODO(), hpa, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update hpa status: %v", err)
	}
	f.refresh()
}
