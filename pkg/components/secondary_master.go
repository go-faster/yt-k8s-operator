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

type secondaryMaster struct {
	componentBase
	server server

	initJob          *InitJob
	exitReadOnlyJob  *InitJob
	adminCredentials corev1.Secret
	spec             *ytv1.MastersSpec
}

func NewSecondaryMaster(cfgen *ytconfig.Generator, ytsaurus *apiproxy.Ytsaurus, spec *ytv1.MastersSpec) Component {
	resource := ytsaurus.GetResource()
	l := labeller.Labeller{
		ObjectMeta:     &resource.ObjectMeta,
		APIProxy:       ytsaurus.APIProxy(),
		ComponentLabel: consts.YTComponentLabelSecondaryMaster,
		ComponentName:  "SecondaryMaster",
		MonitoringPort: consts.MasterMonitoringPort,
		Annotations:    labeller.Join(resource.Spec.ExtraPodAnnotations, spec.ExtraPodAnnotations),
		Labels:         labeller.Join(resource.Spec.ExtraPodLabels, spec.ExtraPodLabels),
	}

	srv := newServer(
		&l,
		ytsaurus,
		&spec.InstanceSpec,
		"/usr/bin/ytserver-master",
		"ytserver-master.yson",
		cfgen.GetMasterCellStatefulSetName(spec.CellTag),
		cfgen.GetSecondaryMastersServiceName(spec.CellTag),
		func() ([]byte, error) { return cfgen.GetMasterCellConfig(spec) },
	)
	initJob := NewInitJob(
		&l,
		ytsaurus.GetJobs(),
		ytsaurus.APIProxy(),
		ytsaurus,
		resource.Spec.ImagePullSecrets,
		"default",
		consts.ClientConfigFileName,
		resource.Spec.CoreImage,
		cfgen.GetNativeClientConfig,
	)

	return &secondaryMaster{
		componentBase: componentBase{
			labeller: &l,
			ytsaurus: ytsaurus,
			cfgen:    cfgen,
		},
		initJob: initJob,
		server:  srv,
		spec:    spec,
	}
}

func (m *secondaryMaster) IsUpdatable() bool {
	return true
}

func (m *secondaryMaster) Fetch(ctx context.Context) error {
	if m.ytsaurus.GetResource().Spec.AdminCredentials != nil {
		if err := m.ytsaurus.APIProxy().FetchObject(ctx,
			m.ytsaurus.GetResource().Spec.AdminCredentials.Name,
			&m.adminCredentials,
		); err != nil {
			return err
		}
	}

	return resources.Fetch(ctx,
		m.server,
		m.initJob,
	)
}

func (m *secondaryMaster) createInitScript() string {
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

func (m *secondaryMaster) doSync(ctx context.Context, dry bool) (ComponentStatus, error) {
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

func (m *secondaryMaster) Status(ctx context.Context) ComponentStatus {
	status, err := m.doSync(ctx, true)
	if err != nil {
		panic(err)
	}

	return status
}

func (m *secondaryMaster) Sync(ctx context.Context) error {
	_, err := m.doSync(ctx, false)
	return err
}

func (m *secondaryMaster) doServerSync(ctx context.Context) error {
	statefulSet := m.server.buildStatefulSet()
	m.addAffinity(statefulSet)
	return m.server.Sync(ctx)
}

func (m *secondaryMaster) addAffinity(statefulSet *appsv1.StatefulSet) {
	if len(m.spec.HostAddresses) == 0 {
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
				Values:   m.spec.HostAddresses,
			},
		},
	})
	nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = selector
	affinity.NodeAffinity = nodeAffinity
	statefulSet.Spec.Template.Spec.Affinity = affinity
}

func (m *secondaryMaster) getHostAddressLabel() string {
	if m.spec.HostAddressLabel != "" {
		return m.spec.HostAddressLabel
	}
	return defaultHostAddressLabel
}

func (m *secondaryMaster) exitReadOnly(ctx context.Context, dry bool) (*ComponentStatus, error) {
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

func (m *secondaryMaster) setMasterReadOnlyExitPrepared(ctx context.Context, status metav1.ConditionStatus) {
	m.ytsaurus.SetUpdateStatusCondition(ctx, metav1.Condition{
		Type:    consts.ConditionMasterExitReadOnlyPrepared,
		Status:  status,
		Reason:  "MasterExitReadOnlyPrepared",
		Message: "Masters are ready to exit read-only state",
	})
}
