package labeller

import (
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ytsaurus/yt-k8s-operator/pkg/apiproxy"
	"github.com/ytsaurus/yt-k8s-operator/pkg/consts"
)

type FetchableObject struct {
	Name   string
	Object client.Object
}

// Join joins multiple maps into one
func Join(mps ...map[string]string) map[string]string {
	result := map[string]string{}
	for _, mp := range mps {
		for k, v := range mp {
			result[k] = v
		}
	}
	return result
}

type Labeller struct {
	APIProxy       apiproxy.APIProxy
	ObjectMeta     *metav1.ObjectMeta
	ComponentLabel string
	ComponentName  string
	MonitoringPort int32
	Annotations    map[string]string
	Labels         map[string]string
}

func (l *Labeller) GetClusterName() string {
	return l.ObjectMeta.Name
}

func (l *Labeller) GetSecretName() string {
	return fmt.Sprintf("%s-secret", l.ComponentLabel)
}

func (l *Labeller) GetMainConfigMapName() string {
	return fmt.Sprintf("%s-config", l.ComponentLabel)
}

func (l *Labeller) GetInitJobName(name string) string {
	return fmt.Sprintf("%s-init-job-%s", l.ComponentLabel, strings.ToLower(name))
}

func (l *Labeller) GetPodsRemovingStartedCondition() string {
	return fmt.Sprintf("%sPodsRemovingStarted", l.ComponentName)
}

func (l *Labeller) GetObjectMeta(name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:        name,
		Namespace:   l.ObjectMeta.Namespace,
		Labels:      l.GetMetaLabelMap(false),
		Annotations: l.Annotations,
	}
}

func (l *Labeller) NeedSync(other metav1.ObjectMeta) bool {
	var (
		otherLabels = other.GetLabels()
		specLabels  = l.GetMetaLabelMap(false)
	)

	for key, value := range specLabels {
		if oldValue, ok := otherLabels[key]; !ok || oldValue != value {
			return true
		}
	}
	return false
}

func (l *Labeller) GetInitJobObjectMeta() metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:        "ytsaurus-init",
		Namespace:   l.ObjectMeta.Namespace,
		Labels:      l.GetMetaLabelMap(true),
		Annotations: l.Annotations,
	}
}

func (l *Labeller) GetYTLabelValue(isInitJob bool) string {
	result := fmt.Sprintf("%s-%s", l.ObjectMeta.Name, l.ComponentLabel)
	if isInitJob {
		result = fmt.Sprintf("%s-%s", result, "init-job")
	}
	return result
}

func (l *Labeller) GetSelectorLabelMap() map[string]string {
	return map[string]string{
		consts.YTComponentLabelName: l.GetYTLabelValue(false),
	}
}

func (l *Labeller) GetListOptions() []client.ListOption {
	return []client.ListOption{
		client.InNamespace(l.ObjectMeta.Namespace),
		client.MatchingLabels(l.GetSelectorLabelMap()),
	}
}

func (l *Labeller) GetMetaLabelMap(isInitJob bool) map[string]string {
	labels := map[string]string{
		"app.kubernetes.io/name":       "Ytsaurus",
		"app.kubernetes.io/instance":   l.ObjectMeta.Name,
		"app.kubernetes.io/component":  l.ComponentLabel,
		"app.kubernetes.io/managed-by": "Ytsaurus-k8s-operator",
		consts.YTComponentLabelName:    l.GetYTLabelValue(isInitJob),
	}
	for k, v := range l.Labels {
		labels[k] = v
	}
	return labels
}

func (l *Labeller) GetMonitoringMetaLabelMap() map[string]string {
	labels := l.GetMetaLabelMap(false)

	labels[consts.YTMetricsLabelName] = "true"

	return labels
}

func GetPodsRemovedCondition(componentName string) string {
	return fmt.Sprintf("%sPodsRemoved", componentName)
}
