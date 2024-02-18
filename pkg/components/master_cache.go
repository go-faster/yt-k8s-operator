package components

import (
	"context"
	"fmt"
	"strings"

	"go.ytsaurus.tech/yt/go/yson"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	ytv1 "github.com/ytsaurus/yt-k8s-operator/api/v1"
	"github.com/ytsaurus/yt-k8s-operator/pkg/apiproxy"
	"github.com/ytsaurus/yt-k8s-operator/pkg/consts"
	"github.com/ytsaurus/yt-k8s-operator/pkg/labeller"
	"github.com/ytsaurus/yt-k8s-operator/pkg/resources"
	"github.com/ytsaurus/yt-k8s-operator/pkg/ytconfig"
)

type masterCache struct {
	componentBase
	server  server
	initJob *InitJob
}

func NewMasterCache(cfgen *ytconfig.Generator, ytsaurus *apiproxy.Ytsaurus) Component {
	resource := ytsaurus.GetResource()
	l := labeller.Labeller{
		ObjectMeta:     &resource.ObjectMeta,
		APIProxy:       ytsaurus.APIProxy(),
		ComponentLabel: consts.YTComponentLabelMasterCache,
		ComponentName:  "MasterCache",
		MonitoringPort: consts.MasterMonitoringPort,
		Annotations:    resource.Spec.ExtraPodAnnotations,
	}

	srv := newServer(
		&l,
		ytsaurus,
		&resource.Spec.MasterCaches.InstanceSpec,
		"/usr/bin/ytserver-master-cache",
		"ytserver-master-cache.yson",
		cfgen.GetMasterCachesStatefulSetName(),
		cfgen.GetMasterCachesServiceName(),
		cfgen.GetMasterCacheConfig,
	)
	initJob := NewInitJob(
		&l,
		ytsaurus.APIProxy(),
		ytsaurus,
		resource.Spec.ImagePullSecrets,
		"default",
		consts.ClientConfigFileName,
		resource.Spec.CoreImage,
		cfgen.GetNativeClientConfig,
	)

	return &masterCache{
		componentBase: componentBase{
			labeller: &l,
			ytsaurus: ytsaurus,
			cfgen:    cfgen,
		},
		server:  srv,
		initJob: initJob,
	}
}

func (m *masterCache) IsUpdatable() bool {
	return true
}

func (m *masterCache) Fetch(ctx context.Context) error {
	return resources.Fetch(ctx,
		m.server,
		m.initJob,
	)
}

func (m *masterCache) getExtraMedia() []Medium {
	mediaMap := make(map[string]Medium)

	for _, d := range m.ytsaurus.GetResource().Spec.DataNodes {
		for _, l := range d.Locations {
			if l.Medium == consts.DefaultMedium {
				continue
			}
			mediaMap[l.Medium] = Medium{
				Name: l.Medium,
			}
		}
	}

	mediaSlice := make([]Medium, 0, len(mediaMap))
	for _, v := range mediaMap {
		mediaSlice = append(mediaSlice, v)
	}

	return mediaSlice
}

func (m *masterCache) initMedia() string {
	var commands []string
	for _, medium := range m.getExtraMedia() {
		attr, err := yson.MarshalFormat(medium, yson.FormatText)
		if err != nil {
			panic(err)
		}
		commands = append(commands, fmt.Sprintf("/usr/bin/yt get //sys/media/%s/@name || /usr/bin/yt create medium --attr '%s'", medium.Name, string(attr)))
	}
	return strings.Join(commands, "\n")
}

func (m *masterCache) doSync(ctx context.Context, dry bool) (ComponentStatus, error) {
	var err error

	if ytv1.IsReadyToUpdateClusterState(m.ytsaurus.GetClusterState()) && m.server.needUpdate() {
		return SimpleStatus(SyncStatusNeedFullUpdate), err
	}

	if m.ytsaurus.GetClusterState() == ytv1.ClusterStateUpdating {
		if status, err := handleUpdatingClusterState(ctx, m.ytsaurus, m, &m.componentBase, m.server, dry); status != nil {
			return *status, err
		}
	}

	if m.server.needSync() {
		if !dry {
			err = m.doServerSync(ctx)
		}
		return WaitingStatus(SyncStatusPending, "components"), err
	}

	if !m.server.arePodsReady(ctx) {
		return WaitingStatus(SyncStatusBlocked, "pods"), err
	}
	if !dry {
		m.initJob.SetInitScript(m.createInitScript())
	}

	return m.initJob.Sync(ctx, dry)
}

func (m *masterCache) Status(ctx context.Context) ComponentStatus {
	status, err := m.doSync(ctx, true)
	if err != nil {
		panic(err)
	}

	return status
}

func (m *masterCache) Sync(ctx context.Context) error {
	_, err := m.doSync(ctx, false)
	return err
}

func (m *masterCache) doServerSync(ctx context.Context) error {
	statefulSet := m.server.buildStatefulSet()
	m.addAffinity(statefulSet)
	return m.server.Sync(ctx)
}

func (m *masterCache) addAffinity(statefulSet *appsv1.StatefulSet) {
	primaryMastersSpec := m.ytsaurus.GetResource().Spec.PrimaryMasters
	if len(primaryMastersSpec.HostAddresses) == 0 {
		return
	}

	affinity := &corev1.Affinity{}
	if statefulSet.Spec.Template.Spec.Affinity != nil {
		affinity = statefulSet.Spec.Template.Spec.Affinity
	}

	nodeAffinity := &corev1.NodeAffinity{}
	if affinity.NodeAffinity != nil {
		nodeAffinity = affinity.NodeAffinity
	}

	selector := &corev1.NodeSelector{}
	if nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		selector = nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	}

	nodeHostnameLabel := m.getHostAddressLabel()
	selector.NodeSelectorTerms = append(selector.NodeSelectorTerms, corev1.NodeSelectorTerm{
		MatchExpressions: []corev1.NodeSelectorRequirement{
			{
				Key:      nodeHostnameLabel,
				Operator: corev1.NodeSelectorOpIn,
				Values:   primaryMastersSpec.HostAddresses,
			},
		},
	})
	nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = selector
	affinity.NodeAffinity = nodeAffinity
	statefulSet.Spec.Template.Spec.Affinity = affinity
}

func (m *masterCache) getHostAddressLabel() string {
	primaryMastersSpec := m.ytsaurus.GetResource().Spec.PrimaryMasters
	if primaryMastersSpec.HostAddressLabel != "" {
		return primaryMastersSpec.HostAddressLabel
	}
	return defaultHostAddressLabel
}

func (m *masterCache) createInitScript() string {
	clusterConnection, err := m.cfgen.GetClusterConnection()
	if err != nil {
		panic(err)
	}

	script := []string{
		initJobWithNativeDriverPrologue(),
		// TODO(ernado): provision lock?
		fmt.Sprintf("/usr/bin/yt set //sys/@cluster_connection '%s'", string(clusterConnection)),
		m.initMedia(),
	}

	return strings.Join(script, "\n")
}
