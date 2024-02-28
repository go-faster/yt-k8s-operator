package resources

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ytsaurus/yt-k8s-operator/pkg/apiproxy"
	"github.com/ytsaurus/yt-k8s-operator/pkg/labeller"
)

type HeadlessService struct {
	name     string
	labeller *labeller.Labeller
	apiProxy apiproxy.APIProxy

	oldObject corev1.Service
	newObject corev1.Service
}

func NewHeadlessService(name string, labeller *labeller.Labeller, apiProxy apiproxy.APIProxy) *HeadlessService {
	return &HeadlessService{
		name:     name,
		labeller: labeller,
		apiProxy: apiProxy,
	}
}

func (s *HeadlessService) OldObject() client.Object {
	return &s.oldObject
}

func (s *HeadlessService) Name() string {
	return s.name
}

func (s *HeadlessService) Sync(ctx context.Context) error {
	return s.apiProxy.SyncObject(ctx, &s.oldObject, &s.newObject)
}

func (s *HeadlessService) Build() *corev1.Service {
	s.newObject.ObjectMeta = s.labeller.GetObjectMeta(s.name)
	s.newObject.Spec = corev1.ServiceSpec{
		ClusterIP: "None",
		Selector:  s.labeller.GetSelectorLabelMap(),
	}

	return &s.newObject
}

func (s *HeadlessService) Fetch(ctx context.Context) error {
	return s.apiProxy.FetchObject(ctx, s.name, &s.oldObject)
}
