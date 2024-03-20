package controllers

import (
	"context"
	"fmt"
	"net/http"
	"regexp"

	"github.com/ytsaurus/yt-k8s-operator/pkg/consts"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

type NodeToPodLabeller struct {
	LabelRegex *regexp.Regexp
}

//+kubebuilder:webhook:path=/mutate-v1-pod-binding,mutating=true,failurePolicy=ignore,sideEffects=None,groups="",resources=pods/binding;,verbs=create;update,versions=v1,name=yt-pod-rack.kb.io,admissionReviewVersions=v1
//+kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list

const apiPathMutatePod = "/mutate-v1-pod-binding"

func (l *NodeToPodLabeller) SetupWebhookWithManager(mgr ctrl.Manager) error {
	client := mgr.GetClient()

	decoder, err := admission.NewDecoder(mgr.GetScheme())
	if err != nil {
		return fmt.Errorf("create decoder: %w", err)
	}

	mgr.GetWebhookServer().Register(apiPathMutatePod, &admission.Webhook{
		Handler: admission.HandlerFunc(func(ctx context.Context, req admission.Request) admission.Response {
			logger := log.FromContext(ctx)

			binding := new(corev1.Binding)
			if err := decoder.Decode(req, binding); err != nil {
				return admission.Errored(http.StatusBadRequest, err)
			}
			var (
				podName = types.NamespacedName{
					Namespace: binding.Namespace,
					Name:      binding.Name,
				}
				nodeName = types.NamespacedName{
					Namespace: binding.Target.Namespace,
					Name:      binding.Target.Name,
				}
			)
			logger.V(1).Info("Received binding admission request",
				"pod", podName,
				"node", nodeName,
			)

			pod := new(corev1.Pod)
			if err := client.Get(ctx, podName, pod); err != nil {
				logger.Error(err, "get pod",
					"namespace", podName.Namespace,
					"name", podName.Name,
				)
				return admission.Errored(http.StatusInternalServerError, err)
			}

			if _, ok := pod.Labels[consts.YTComponentLabelName]; !ok {
				logger.V(1).Info("Pod is not YT component, ignoring",
					"pod", podName,
				)
				return admission.Allowed("pod is not YT component")
			}

			node := new(corev1.Node)
			if err := client.Get(ctx, nodeName, node); err != nil {
				logger.Error(err, "get node",
					"namespace", nodeName.Namespace,
					"name", nodeName.Name,
				)
				return admission.Errored(http.StatusInternalServerError, err)
			}

			pod = pod.DeepCopy()
			var patched int
			for name, value := range node.Labels {
				if !l.LabelRegex.MatchString(name) {
					continue
				}

				if pod.Labels == nil {
					pod.Labels = map[string]string{}
				}
				pod.Labels[name] = value
				patched++
			}

			logger.V(1).Info("Patching pod",
				"pod", podName,
				"node", nodeName,
				"patched", patched,
			)

			if err := client.Update(ctx, pod); err != nil {
				logger.Error(err, "patch pod",
					"pod", podName,
				)
				return admission.Errored(http.StatusInternalServerError, err)
			}
			return admission.Allowed("pod patched")
		}),
	})

	return nil
}
