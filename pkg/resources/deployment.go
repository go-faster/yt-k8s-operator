package resources

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/google/go-cmp/cmp"
	"github.com/ytsaurus/yt-k8s-operator/pkg/apiproxy"
	"github.com/ytsaurus/yt-k8s-operator/pkg/labeller"
)

type Deployment struct {
	name     string
	labeller *labeller.Labeller
	ytsaurus *apiproxy.Ytsaurus

	oldObject    appsv1.Deployment
	newObject    appsv1.Deployment
	built        bool
	nodeSelector map[string]string
	tolerations  []corev1.Toleration
	affinity     *corev1.Affinity
}

func NewDeployment(
	name string,
	labeller *labeller.Labeller,
	tolerations []corev1.Toleration,
	affinity *corev1.Affinity,
	nodeSelector map[string]string,
	ytsaurus *apiproxy.Ytsaurus,
) *Deployment {
	return &Deployment{
		name:         name,
		labeller:     labeller,
		ytsaurus:     ytsaurus,
		tolerations:  tolerations,
		affinity:     affinity,
		nodeSelector: nodeSelector,
	}
}

func (d *Deployment) OldObject() client.Object {
	return &d.oldObject
}

func (d *Deployment) Name() string {
	return d.name
}

func (d *Deployment) Sync(ctx context.Context) error {
	return d.ytsaurus.APIProxy().SyncObject(ctx, &d.oldObject, &d.newObject)
}

func (d *Deployment) Build() *appsv1.Deployment {
	if !d.built {
		d.newObject.ObjectMeta = d.labeller.GetObjectMeta(d.name)
		d.newObject.Spec = appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: d.labeller.GetSelectorLabelMap(),
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      d.labeller.GetMetaLabelMap(false),
					Annotations: d.ytsaurus.GetResource().Spec.ExtraPodAnnotations,
				},
				Spec: corev1.PodSpec{
					Tolerations:      d.tolerations,
					Affinity:         d.affinity,
					NodeSelector:     d.nodeSelector,
					ImagePullSecrets: d.ytsaurus.GetResource().Spec.ImagePullSecrets,
				},
			},
		}
	}

	d.built = true
	return &d.newObject
}

func (d *Deployment) equalManagedPodSpec(other corev1.PodSpec) bool {
	return cmp.Equal(other.Tolerations, d.tolerations) &&
		cmp.Equal(other.Affinity, d.affinity) &&
		cmp.Equal(other.NodeSelector, d.nodeSelector)
}

func (d *Deployment) NeedSync(replicas int32) bool {
	if !d.labeller.EqualObjectMeta(d.name, d.oldObject.ObjectMeta) {
		return true
	}
	oldSpec := d.oldObject.Spec
	if oldReplicas := oldSpec.Replicas; oldReplicas == nil || *oldReplicas != replicas {
		return true
	}
	oldTemplateSpec := oldSpec.Template.Spec
	if !d.equalManagedPodSpec(oldTemplateSpec) {
		return true
	}
	return len(oldTemplateSpec.Containers) != 1
}

func (d *Deployment) ArePodsRemoved(ctx context.Context) bool {
	return d.oldObject.Status.AvailableReplicas == 0 && d.oldObject.Status.Replicas == 0
}

func (d *Deployment) ArePodsReady(ctx context.Context) bool {
	logger := log.FromContext(ctx)

	if d.oldObject.Spec.Replicas == nil {
		logger.Error(nil,
			"desired number of pods is not specified", "deployment", d.name)
		return false
	}

	if *d.oldObject.Spec.Replicas != d.oldObject.Status.Replicas {
		logger.Info("desired number of pods is not equal to actual yet",
			"deployment", d.name,
			"desiredNumberOfPods", *d.oldObject.Spec.Replicas,
			"actualNumberOfPods", d.oldObject.Status.Replicas,
		)
		return false
	}

	if d.oldObject.Status.AvailableReplicas != d.oldObject.Status.Replicas {
		logger.Info("total number of pods is not equal to number of running ones yet",
			"deployment", d.name,
			"totalNumberOfPods", d.oldObject.Status.Replicas,
			"numberOfRunningPods", d.oldObject.Status.AvailableReplicas,
		)
		return false
	}

	return true
}

func (d *Deployment) Fetch(ctx context.Context) error {
	return d.ytsaurus.APIProxy().FetchObject(ctx, d.name, &d.oldObject)
}
