package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

type NodeToPodLabeller struct {
	Labels map[string]struct{}
}

//+kubebuilder:webhook:path=/mutate-v1-pod,mutating=true,failurePolicy=fail,sideEffects=None,groups="",resources=pods/binding;,verbs=create;update,versions=v1,name=yt-pod-rack.kb.io,admissionReviewVersions=v1
//+kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list

const apiPathMutatePod = "/mutate-v1-pod"

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

			node := new(corev1.Node)
			if err := client.Get(ctx, nodeName, node); err != nil {
				logger.Error(err, "get node",
					"namespace", nodeName.Namespace,
					"name", nodeName.Name,
				)
				return admission.Errored(http.StatusInternalServerError, err)
			}

			var patched int
			for name := range l.Labels {
				if val, ok := node.Labels[name]; ok {
					if pod.Labels == nil {
						pod.Labels = map[string]string{}
					}
					patched++
					pod.Labels[name] = val
				}
			}
			logger.V(1).Info("Patching pod",
				"pod", pod.Name,
				"pod_namespace", pod.Namespace,
				"node", nodeName,
				"patched", patched,
			)

			marshaledPod, err := json.Marshal(pod)
			if err != nil {
				return admission.Errored(http.StatusInternalServerError, err)
			}
			return admission.PatchResponseFromRaw(req.Object.Raw, marshaledPod)
		}),
	})

	return nil
}