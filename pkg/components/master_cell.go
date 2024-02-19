package components

import (
	"context"
	"fmt"
	"strings"

	"go.ytsaurus.tech/library/go/ptr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ytv1 "github.com/ytsaurus/yt-k8s-operator/api/v1"
	"github.com/ytsaurus/yt-k8s-operator/pkg/apiproxy"
	"github.com/ytsaurus/yt-k8s-operator/pkg/consts"
	"github.com/ytsaurus/yt-k8s-operator/pkg/labeller"
	"github.com/ytsaurus/yt-k8s-operator/pkg/resources"
	"github.com/ytsaurus/yt-k8s-operator/pkg/ytconfig"
)

type masterCell struct {
	componentBase
	server server

	initJob          *InitJob
	exitReadOnlyJob  *InitJob
	adminCredentials corev1.Secret
}

func NewMasterCell(cfgen *ytconfig.Generator, ytsaurus *apiproxy.Ytsaurus, spec *ytv1.MastersSpec) Component {
	resource := ytsaurus.GetResource()
	l := labeller.Labeller{
		ObjectMeta:     &resource.ObjectMeta,
		APIProxy:       ytsaurus.APIProxy(),
		ComponentLabel: consts.YTComponentLabelMasterCell,
		ComponentName:  "MasterCell",
		MonitoringPort: consts.MasterMonitoringPort,
		Annotations:    resource.Spec.ExtraPodAnnotations,
	}

	srv := newServer(
		&l,
		ytsaurus,
		&spec.InstanceSpec,
		"/usr/bin/ytserver-master",
		"ytserver-master.yson",
		cfgen.GetMasterCellStatefulSetName(spec.CellTag),
		cfgen.GetMasterCellServiceName(spec.CellTag),
		func() ([]byte, error) { return cfgen.GetMasterCellConfig(spec) },
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

	return &masterCell{
		componentBase: componentBase{
			labeller: &l,
			ytsaurus: ytsaurus,
			cfgen:    cfgen,
		},
		initJob: initJob,
		server:  srv,
	}
}

func (m *masterCell) IsUpdatable() bool {
	return true
}

func (m *masterCell) Fetch(ctx context.Context) error {
	if m.ytsaurus.GetResource().Spec.AdminCredentials != nil {
		err := m.ytsaurus.APIProxy().FetchObject(
			ctx,
			m.ytsaurus.GetResource().Spec.AdminCredentials.Name,
			&m.adminCredentials)
		if err != nil {
			return err
		}
	}

	return resources.Fetch(ctx,
		m.server,
		m.initJob,
	)
}

func (m *masterCell) getExtraMedia() []Medium {
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

func (m *masterCell) createInitScript() string {
	clusterConnection, err := m.cfgen.GetClusterConnection()
	if err != nil {
		panic(err)
	}

	script := []string{
		initJobWithNativeDriverPrologue(),
		fmt.Sprintf("/usr/bin/yt set //sys/@cluster_connection '%s'", string(clusterConnection)),
	}

	return strings.Join(script, "\n")
}

func (m *masterCell) doSync(ctx context.Context, dry bool) (ComponentStatus, error) {
	var err error

	if ytv1.IsReadyToUpdateClusterState(m.ytsaurus.GetClusterState()) && m.server.needUpdate() {
		return SimpleStatus(SyncStatusNeedFullUpdate), err
	}

	if m.ytsaurus.GetClusterState() == ytv1.ClusterStateUpdating {
		if m.ytsaurus.GetUpdateState() == ytv1.UpdateStateWaitingForMasterExitReadOnly {
			st, err := m.exitReadOnly(ctx, dry)
			return *st, err
		}
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

func (m *masterCell) Status(ctx context.Context) ComponentStatus {
	status, err := m.doSync(ctx, true)
	if err != nil {
		panic(err)
	}

	return status
}

func (m *masterCell) Sync(ctx context.Context) error {
	_, err := m.doSync(ctx, false)
	return err
}

func (m *masterCell) doServerSync(ctx context.Context) error {
	statefulSet := m.server.buildStatefulSet()
	m.addAffinity(statefulSet)
	return m.server.Sync(ctx)
}

func (m *masterCell) addAffinity(statefulSet *appsv1.StatefulSet) {
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

func (m *masterCell) getHostAddressLabel() string {
	primaryMastersSpec := m.ytsaurus.GetResource().Spec.PrimaryMasters
	if primaryMastersSpec.HostAddressLabel != "" {
		return primaryMastersSpec.HostAddressLabel
	}
	return defaultHostAddressLabel
}

func (m *masterCell) exitReadOnly(ctx context.Context, dry bool) (*ComponentStatus, error) {
	if !m.ytsaurus.IsUpdateStatusConditionTrue(consts.ConditionMasterExitReadOnlyPrepared) {
		if !m.exitReadOnlyJob.isRestartPrepared() {
			if err := m.exitReadOnlyJob.prepareRestart(ctx, dry); err != nil {
				return ptr.T(SimpleStatus(SyncStatusUpdating)), err
			}
		}

		if !dry {
			m.setMasterReadOnlyExitPrepared(ctx, metav1.ConditionTrue)
		}
		return ptr.T(SimpleStatus(SyncStatusUpdating)), nil
	}

	if !dry {
		m.ytsaurus.SetUpdateStatusCondition(ctx, metav1.Condition{
			Type:    consts.ConditionMasterExitedReadOnly,
			Status:  metav1.ConditionTrue,
			Reason:  "MasterExitedReadOnly",
			Message: "Masters exited read-only state",
		})
		m.setMasterReadOnlyExitPrepared(ctx, metav1.ConditionFalse)
	}
	return ptr.T(SimpleStatus(SyncStatusUpdating)), nil
}

func (m *masterCell) setMasterReadOnlyExitPrepared(ctx context.Context, status metav1.ConditionStatus) {
	m.ytsaurus.SetUpdateStatusCondition(ctx, metav1.Condition{
		Type:    consts.ConditionMasterExitReadOnlyPrepared,
		Status:  status,
		Reason:  "MasterExitReadOnlyPrepared",
		Message: "Masters are ready to exit read-only state",
	})
}
