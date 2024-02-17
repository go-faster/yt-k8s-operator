package ytconfig

import (
	ytv1 "github.com/ytsaurus/yt-k8s-operator/api/v1"
	"github.com/ytsaurus/yt-k8s-operator/pkg/consts"
)

type MasterCacheServer struct {
	CommonServer
	PrimaryMaster MasterCell `yson:"primary_master"`
}

func getMasterCacheLogging(spec *ytv1.MasterCachesSpec) Logging {
	return createLogging(
		&spec.InstanceSpec,
		"master-cache",
		[]ytv1.TextLoggerSpec{defaultInfoLoggerSpec(), defaultStderrLoggerSpec()},
	)
}

func getMasterCacheServerCarcass(spec *ytv1.MasterCachesSpec) (MasterCacheServer, error) {
	var c MasterCacheServer
	c.RPCPort = consts.MasterRPCPort
	c.MonitoringPort = consts.MasterMonitoringPort

	c.Logging = getMasterCacheLogging(spec)

	return c, nil
}
