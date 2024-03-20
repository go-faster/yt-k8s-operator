package components

import (
	"context"

	ytv1 "github.com/ytsaurus/yt-k8s-operator/api/v1"
	"github.com/ytsaurus/yt-k8s-operator/pkg/apiproxy"
	"github.com/ytsaurus/yt-k8s-operator/pkg/consts"
	"github.com/ytsaurus/yt-k8s-operator/pkg/labeller"
	"github.com/ytsaurus/yt-k8s-operator/pkg/resources"
	"github.com/ytsaurus/yt-k8s-operator/pkg/ytconfig"
)

type dataNode struct {
	componentBase
	server server
	master Component

	yc   YtsaurusClient
	rack *rackSetup
}

func NewDataNode(
	cfgen *ytconfig.Generator,
	ytsaurus *apiproxy.Ytsaurus,
	yc YtsaurusClient,
	master Component,
	spec ytv1.DataNodesSpec,
) Component {
	resource := ytsaurus.GetResource()
	l := labeller.Labeller{
		ObjectMeta:     &resource.ObjectMeta,
		APIProxy:       ytsaurus.APIProxy(),
		ComponentLabel: cfgen.FormatComponentStringWithDefault(consts.YTComponentLabelDataNode, spec.Name),
		ComponentName:  cfgen.FormatComponentStringWithDefault("DataNode", spec.Name),
		MonitoringPort: consts.DataNodeMonitoringPort,
	}

	server := newServer(
		&l,
		ytsaurus,
		&spec.InstanceSpec,
		"/usr/bin/ytserver-node",
		"ytserver-data-node.yson",
		cfgen.GetDataNodesStatefulSetName(spec.Name),
		cfgen.GetDataNodesServiceName(spec.Name),
		func() ([]byte, error) {
			return cfgen.GetDataNodeConfig(spec)
		},
	)

	rack := newRackSetup(
		ytsaurus,
		l,
		consts.DataNodeRPCPort,
	)

	return &dataNode{
		componentBase: componentBase{
			labeller: &l,
			ytsaurus: ytsaurus,
			cfgen:    cfgen,
		},
		server: server,
		master: master,
		yc:     yc,
		rack:   rack,
	}
}

func (n *dataNode) IsUpdatable() bool {
	return true
}

func (n *dataNode) Fetch(ctx context.Context) error {
	return resources.Fetch(ctx, n.server)
}

func (n *dataNode) doSync(ctx context.Context, dry bool) (ComponentStatus, error) {
	var err error

	if ytv1.IsReadyToUpdateClusterState(n.ytsaurus.GetClusterState()) && n.server.needUpdate() {
		return SimpleStatus(SyncStatusNeedFullUpdate), err
	}

	if n.ytsaurus.GetClusterState() == ytv1.ClusterStateUpdating {
		if status, err := handleUpdatingClusterState(ctx, n.ytsaurus, n, &n.componentBase, n.server, dry); status != nil {
			return *status, err
		}
	}

	if !IsRunningStatus(n.master.Status(ctx).SyncStatus) {
		return WaitingStatus(SyncStatusBlocked, n.master.GetName()), err
	}

	if n.server.needSync() {
		if !dry {
			err = n.server.Sync(ctx)
		}
		return WaitingStatus(SyncStatusPending, "components"), err
	}

	if !n.server.arePodsReady(ctx) {
		return WaitingStatus(SyncStatusBlocked, "pods"), err
	}

	if n.yc.Status(ctx).SyncStatus != SyncStatusReady {
		return WaitingStatus(SyncStatusBlocked, n.yc.GetName()), err
	}
	if err := n.rack.SetRacks(ctx, n.yc.GetYtClient()); err != nil {
		return WaitingStatus(SyncStatusBlocked, "rack_awareness"), err
	}

	return SimpleStatus(SyncStatusReady), err
}

func (n *dataNode) Status(ctx context.Context) ComponentStatus {
	status, err := n.doSync(ctx, true)
	if err != nil {
		panic(err)
	}

	return status
}

func (n *dataNode) Sync(ctx context.Context) error {
	_, err := n.doSync(ctx, false)
	return err
}
