package ytconfig

import (
	"fmt"
	"path"

	"go.ytsaurus.tech/yt/go/yson"
	corev1 "k8s.io/api/core/v1"
	ptr "k8s.io/utils/pointer"

	ytv1 "github.com/ytsaurus/yt-k8s-operator/api/v1"
	"github.com/ytsaurus/yt-k8s-operator/pkg/consts"
)

type ConfigFormat string

const (
	ConfigFormatYSON               = "yson"
	ConfigFormatJSON               = "json"
	ConfigFormatJSONWithJSPrologue = "json_with_js_prologue"
)

type YsonGeneratorFunc func() ([]byte, error)
type GeneratorDescriptor struct {
	// F must generate config in YSON.
	F YsonGeneratorFunc
	// Fmt is the desired serialization format for config map.
	// Note that conversion from YSON to Fmt (if needed) is performed as a very last
	// step of config generation pipeline.
	Fmt ConfigFormat
}

type Generator struct {
	ytsaurus      *ytv1.Ytsaurus
	clusterDomain string
}

func NewGenerator(ytsaurus *ytv1.Ytsaurus, clusterDomain string) *Generator {
	return &Generator{
		ytsaurus:      ytsaurus,
		clusterDomain: clusterDomain,
	}
}

func (g *Generator) getMasterCachePodFQDNSuffix() string {
	return fmt.Sprintf("%s.%s.svc.%s",
		g.GetMasterCachesServiceName(),
		g.ytsaurus.Namespace,
		g.clusterDomain,
	)
}

func (g *Generator) getMasterCacheAddresses() []string {
	hosts := g.ytsaurus.Spec.MasterCaches.HostAddresses

	if len(hosts) == 0 {
		masterPodSuffix := g.getMasterCachePodFQDNSuffix()
		for _, podName := range g.GetMasterCachePodNames() {
			hosts = append(hosts, fmt.Sprintf("%s.%s",
				podName,
				masterPodSuffix,
			))
		}
	}

	addresses := make([]string, len(hosts))
	for idx, host := range hosts {
		addresses[idx] = fmt.Sprintf("%s:%d", host, consts.MasterRPCPort)
	}
	return addresses
}

func (g *Generator) getMasterPodFqdnSuffix() string {
	return fmt.Sprintf("%s.%s.svc.%s",
		g.GetMastersServiceName(),
		g.ytsaurus.Namespace,
		g.clusterDomain)
}

func (g *Generator) getSecondaryMasterPodFQDNSuffix(spec ytv1.MastersSpec) string {
	return fmt.Sprintf("%s.%s.svc.%s",
		g.GetSecondaryMastersServiceName(spec.CellTag),
		g.ytsaurus.Namespace,
		g.clusterDomain,
	)
}

func (g *Generator) getSecondaryMasterAddresses(spec ytv1.MastersSpec) []string {
	hosts := spec.HostAddresses
	if len(hosts) == 0 {
		masterPodSuffix := g.getSecondaryMasterPodFQDNSuffix(spec)
		for _, podName := range g.GetSecondaryMastersPodNames(spec) {
			hosts = append(hosts, fmt.Sprintf("%s.%s",
				podName,
				masterPodSuffix,
			))
		}
	}
	addresses := make([]string, len(hosts))
	for idx, host := range hosts {
		addresses[idx] = fmt.Sprintf("%s:%d", host, consts.MasterRPCPort)
	}
	return addresses
}

func (g *Generator) getMasterAddresses() []string {
	hosts := g.ytsaurus.Spec.PrimaryMasters.HostAddresses

	if len(hosts) == 0 {
		masterPodSuffix := g.getMasterPodFqdnSuffix()
		for _, podName := range g.GetMasterPodNames() {
			hosts = append(hosts, fmt.Sprintf("%s.%s",
				podName,
				masterPodSuffix,
			))
		}
	}

	addresses := make([]string, len(hosts))
	for idx, host := range hosts {
		addresses[idx] = fmt.Sprintf("%s:%d", host, consts.MasterRPCPort)
	}
	return addresses
}

func (g *Generator) getMasterHydraPeers() []HydraPeer {
	peers := make([]HydraPeer, 0, g.ytsaurus.Spec.PrimaryMasters.InstanceCount)
	for _, address := range g.getMasterAddresses() {
		peers = append(peers, HydraPeer{
			Address: address,
			Voting:  true,
		})
	}
	return peers
}

func (g *Generator) getSecondaryMasterHydraPeers(spec ytv1.MastersSpec) []HydraPeer {
	peers := make([]HydraPeer, 0, spec.InstanceCount)
	for _, address := range g.getSecondaryMasterAddresses(spec) {
		peers = append(peers, HydraPeer{
			Address: address,
			Voting:  true,
		})
	}
	return peers
}

func (g *Generator) getDiscoveryAddresses() []string {
	names := make([]string, 0, g.ytsaurus.Spec.Discovery.InstanceCount)
	for _, podName := range g.GetDiscoveryPodNames() {
		names = append(names, fmt.Sprintf("%s.%s.%s.svc.%s:%d",
			podName,
			g.GetDiscoveryServiceName(),
			g.ytsaurus.Namespace,
			g.clusterDomain,
			consts.DiscoveryRPCPort))
	}
	return names
}

func (g *Generator) GetYQLAgentAddresses() []string {
	names := make([]string, 0, g.ytsaurus.Spec.YQLAgents.InstanceCount)
	for _, podName := range g.GetYQLAgentPodNames() {
		names = append(names, fmt.Sprintf("%s.%s.%s.svc.%s:%d",
			podName,
			g.GetYQLAgentServiceName(),
			g.ytsaurus.Namespace,
			g.clusterDomain,
			consts.YQLAgentRPCPort))
	}
	return names
}

func (g *Generator) GetQueueAgentAddresses() []string {
	names := make([]string, 0, g.ytsaurus.Spec.QueueAgents.InstanceCount)
	for _, podName := range g.GetQueueAgentPodNames() {
		names = append(names, fmt.Sprintf("%s.%s.%s.svc.%s:%d",
			podName,
			g.GetQueueAgentServiceName(),
			g.ytsaurus.Namespace,
			g.clusterDomain,
			consts.QueueAgentRPCPort))
	}
	return names
}

func (g *Generator) fillDriver(c *Driver) {
	c.TimestampProviders.Addresses = g.getMasterAddresses()

	c.PrimaryMaster.Addresses = g.getMasterAddresses()
	c.PrimaryMaster.CellID = generateCellID(g.ytsaurus.Spec.PrimaryMasters.CellTag)

	c.MasterCache = g.getMasterCache()
}

func (g *Generator) fillAddressResolver(c *AddressResolver) {
	var retries = 1000

	c.EnableIPv4 = g.ytsaurus.Spec.UseIPv4
	c.EnableIPv6 = g.ytsaurus.Spec.UseIPv6
	if !c.EnableIPv6 && !c.EnableIPv4 {
		// In case when nothing is specified, we prefer IPv4 due to compatibility reasons.
		c.EnableIPv4 = true
	}
	c.Retries = &retries
}

func (g *Generator) getMasterCache() *MasterCache {
	addr := g.getMasterCacheAddresses()
	if len(addr) == 0 {
		return nil
	}
	var c MasterCache
	c.Addresses = addr
	c.CellID = generateCellID(g.ytsaurus.Spec.MasterCaches.CellTag)
	c.EnableMasterCacheDiscover = false
	return &c
}

func (g *Generator) fillPrimaryMaster(c *MasterCell) {
	c.Addresses = g.getMasterAddresses()
	c.Peers = g.getMasterHydraPeers()
	c.CellID = generateCellID(g.ytsaurus.Spec.PrimaryMasters.CellTag)
}

func (g *Generator) getSecondaryMasters() []MasterCell {
	var cells []MasterCell
	for _, spec := range g.ytsaurus.Spec.SecondaryMasters {
		c := MasterCell{}
		c.Addresses = g.getSecondaryMasterAddresses(spec)
		c.CellID = generateCellID(spec.CellTag)
		c.Peers = g.getSecondaryMasterHydraPeers(spec)
		cells = append(cells, c)
	}
	return cells
}

func (g *Generator) fillClusterConnection(c *ClusterConnection, s *ytv1.RPCTransportSpec) {
	g.fillPrimaryMaster(&c.PrimaryMaster)
	c.SecondaryMasters = g.getSecondaryMasters()
	c.MasterCache = g.getMasterCache()
	c.ClusterName = g.ytsaurus.Name
	c.DiscoveryConnection.Addresses = g.getDiscoveryAddresses()
	g.fillClusterConnectionEncryption(c, s)
}

func (g *Generator) fillCypressAnnotations(c *map[string]any) {
	*c = map[string]any{
		"k8s_pod_name":      "{K8S_POD_NAME}",
		"k8s_pod_namespace": "{K8S_POD_NAMESPACE}",
		"k8s_node_name":     "{K8S_NODE_NAME}",
	}
}

func (g *Generator) fillCommonService(c *CommonServer, s *ytv1.InstanceSpec) {
	// ToDo(psushin): enable porto resource tracker?
	g.fillAddressResolver(&c.AddressResolver)
	g.fillClusterConnection(&c.ClusterConnection, s.NativeTransport)
	g.fillCypressAnnotations(&c.CypressAnnotations)
	c.TimestampProviders.Addresses = g.getMasterAddresses()
}

func (g *Generator) fillBusEncryption(b *Bus, s *ytv1.RPCTransportSpec) {
	if s.TLSRequired {
		b.EncryptionMode = EncryptionModeRequired
	} else {
		b.EncryptionMode = EncryptionModeOptional
	}

	b.CertChain = &PemBlob{
		FileName: path.Join(consts.RPCSecretMountPoint, corev1.TLSCertKey),
	}
	b.PrivateKey = &PemBlob{
		FileName: path.Join(consts.RPCSecretMountPoint, corev1.TLSPrivateKeyKey),
	}
}

func (g *Generator) fillBusServer(c *CommonServer, s *ytv1.RPCTransportSpec) {
	if s == nil {
		// Use common bus transport config
		s = g.ytsaurus.Spec.NativeTransport
	}
	if s == nil || s.TLSSecret == nil {
		return
	}

	if c.BusServer == nil {
		c.BusServer = &BusServer{}
	}

	// FIXME(khlebnikov): some clients does not support TLS yet
	if s.TLSRequired && s != g.ytsaurus.Spec.NativeTransport {
		c.BusServer.EncryptionMode = EncryptionModeRequired
	} else {
		c.BusServer.EncryptionMode = EncryptionModeOptional
	}

	c.BusServer.CertChain = &PemBlob{
		FileName: path.Join(consts.BusSecretMountPoint, corev1.TLSCertKey),
	}
	c.BusServer.PrivateKey = &PemBlob{
		FileName: path.Join(consts.BusSecretMountPoint, corev1.TLSPrivateKeyKey),
	}
}

func (g *Generator) fillClusterConnectionEncryption(c *ClusterConnection, s *ytv1.RPCTransportSpec) {
	if s == nil {
		// Use common bus transport config
		s = g.ytsaurus.Spec.NativeTransport
	}
	if s == nil || s.TLSSecret == nil {
		return
	}

	if c.BusClient == nil {
		c.BusClient = &Bus{}
	}

	if g.ytsaurus.Spec.CABundle != nil {
		c.BusClient.CA = &PemBlob{
			FileName: path.Join(consts.CABundleMountPoint, consts.CABundleFileName),
		}
	} else {
		c.BusClient.CA = &PemBlob{
			FileName: consts.DefaultCABundlePath,
		}
	}

	if s.TLSRequired {
		c.BusClient.EncryptionMode = EncryptionModeRequired
	} else {
		c.BusClient.EncryptionMode = EncryptionModeOptional
	}

	if s.TLSInsecure {
		c.BusClient.VerificationMode = VerificationModeNone
	} else {
		c.BusClient.VerificationMode = VerificationModeFull
	}

	if s.TLSPeerAlternativeHostName != "" {
		c.BusClient.PeerAlternativeHostName = s.TLSPeerAlternativeHostName
	}
}

func marshallYSONConfig(c interface{}) ([]byte, error) {
	result, err := yson.MarshalFormat(c, yson.FormatPretty)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (g *Generator) GetClusterConnection() ([]byte, error) {
	var c ClusterConnection
	g.fillClusterConnection(&c, nil)
	return marshallYSONConfig(c)
}

func (g *Generator) GetStrawberryControllerConfig() ([]byte, error) {
	c := getStrawberryController()
	proxy := g.GetHTTPProxiesAddress(consts.DefaultHTTPProxyRole)
	c.LocationProxies = []string{proxy}
	c.HTTPLocationAliases = map[string][]string{
		proxy: []string{g.ytsaurus.Name},
	}
	return marshallYSONConfig(c)
}

func (g *Generator) GetChytInitClusterConfig() ([]byte, error) {
	c := getChytInitCluster()
	c.Proxy = g.GetHTTPProxiesAddress(consts.DefaultHTTPProxyRole)
	return marshallYSONConfig(c)
}

func (g *Generator) getMasterConfig(spec *ytv1.MastersSpec) (MasterServer, error) {
	c, err := getMasterServerCarcass(spec)
	if err != nil {
		return MasterServer{}, err
	}
	g.fillCommonService(&c.CommonServer, &spec.InstanceSpec)
	g.fillBusServer(&c.CommonServer, spec.NativeTransport)
	g.fillPrimaryMaster(&c.PrimaryMaster)
	c.SecondaryMasters = g.getSecondaryMasters()
	configureMasterServerCypressManager(g.ytsaurus.Spec, &c.CypressManager)

	// COMPAT(l0kix2): remove that after we drop support for specifying host network without master host addresses.
	if g.ytsaurus.Spec.HostNetwork && len(spec.HostAddresses) == 0 {
		// Each master deduces its index within cell by looking up his FQDN in the
		// list of all master peers. Master peers are specified using their pod addresses,
		// therefore we must also switch masters from identifying themselves by FQDN addresses
		// to their pod addresses.

		// POD_NAME is set to pod name through downward API env var and substituted during
		// config postprocessing.
		c.AddressResolver.LocalhostNameOverride = ptr.String(
			fmt.Sprintf("%v.%v", "{K8S_POD_NAME}", g.getMasterPodFqdnSuffix()))
	}

	return c, nil
}

func (g *Generator) GetMasterCellConfig(spec *ytv1.MastersSpec) ([]byte, error) {
	c, err := g.getMasterConfig(spec)
	if err != nil {
		return nil, err
	}
	return marshallYSONConfig(c)
}

func (g *Generator) GetMasterConfig() ([]byte, error) {
	return g.GetMasterCellConfig(&g.ytsaurus.Spec.PrimaryMasters)
}

func (g *Generator) getMasterCacheConfigImpl() (MasterCacheServer, error) {
	spec := &g.ytsaurus.Spec.MasterCaches
	c, err := getMasterCacheServerCarcass(spec)
	if err != nil {
		return MasterCacheServer{}, err
	}
	g.fillCommonService(&c.CommonServer, &spec.InstanceSpec)
	g.fillBusServer(&c.CommonServer, spec.NativeTransport)
	g.fillPrimaryMaster(&c.PrimaryMaster)

	// COMPAT(l0kix2): remove that after we drop support for specifying host network without master host addresses.
	if g.ytsaurus.Spec.HostNetwork && len(spec.HostAddresses) == 0 {
		// Each master deduces its index within cell by looking up his FQDN in the
		// list of all master peers. Master peers are specified using their pod addresses,
		// therefore we must also switch masters from identifying themselves by FQDN addresses
		// to their pod addresses.

		// POD_NAME is set to pod name through downward API env var and substituted during
		// config postprocessing.
		c.AddressResolver.LocalhostNameOverride = ptr.String(
			fmt.Sprintf("%v.%v", "{K8S_POD_NAME}", g.getMasterPodFqdnSuffix()))
	}

	return c, nil
}

func (g *Generator) GetMasterCacheConfig() ([]byte, error) {
	c, err := g.getMasterCacheConfigImpl()
	if err != nil {
		return nil, err
	}
	return marshallYSONConfig(c)
}

func (g *Generator) GetNativeClientConfig() ([]byte, error) {
	c, err := getNativeClientCarcass()
	if err != nil {
		return nil, err
	}

	g.fillDriver(&c.Driver)
	g.fillAddressResolver(&c.AddressResolver)
	c.Driver.APIVersion = 4

	return marshallYSONConfig(c)
}

func (g *Generator) getSchedulerConfigImpl() (SchedulerServer, error) {
	spec := g.ytsaurus.Spec.Schedulers
	c, err := getSchedulerServerCarcass(spec)
	if err != nil {
		return SchedulerServer{}, err
	}

	if g.ytsaurus.Spec.TabletNodes == nil || len(g.ytsaurus.Spec.TabletNodes) == 0 {
		c.Scheduler.OperationsCleaner.EnableOperationArchivation = ptr.Bool(false)
	}
	g.fillCommonService(&c.CommonServer, &spec.InstanceSpec)
	g.fillBusServer(&c.CommonServer, spec.NativeTransport)
	return c, nil
}

func (g *Generator) GetSchedulerConfig() ([]byte, error) {
	if g.ytsaurus.Spec.Schedulers == nil {
		return []byte{}, nil
	}

	c, err := g.getSchedulerConfigImpl()
	if err != nil {
		return nil, err
	}

	return marshallYSONConfig(c)
}

func (g *Generator) getRPCProxyConfigImpl(spec *ytv1.RPCProxiesSpec) (RPCProxyServer, error) {
	c, err := getRPCProxyServerCarcass(spec)
	if err != nil {
		return RPCProxyServer{}, err
	}

	g.fillCommonService(&c.CommonServer, &spec.InstanceSpec)

	if g.ytsaurus.Spec.OauthService != nil {
		c.CypressUserManager = CypressUserManager{}
		c.OauthService = &OauthService{
			Host:               g.ytsaurus.Spec.OauthService.Host,
			Port:               g.ytsaurus.Spec.OauthService.Port,
			Secure:             g.ytsaurus.Spec.OauthService.Secure,
			UserInfoEndpoint:   g.ytsaurus.Spec.OauthService.UserInfo.Endpoint,
			UserInfoLoginField: g.ytsaurus.Spec.OauthService.UserInfo.LoginField,
			UserInfoErrorField: g.ytsaurus.Spec.OauthService.UserInfo.ErrorField,
		}
		c.OauthTokenAuthenticator = &OauthTokenAuthenticator{}
		c.RequireAuthentication = true
	}

	return c, nil
}

func (g *Generator) GetRPCProxyConfig(spec ytv1.RPCProxiesSpec) ([]byte, error) {
	c, err := g.getRPCProxyConfigImpl(&spec)
	if err != nil {
		return []byte{}, err
	}

	if spec.Transport.TLSSecret != nil {
		if c.BusServer == nil {
			c.BusServer = &BusServer{}
		}
		g.fillBusEncryption(&c.BusServer.Bus, &spec.Transport)
	}

	return marshallYSONConfig(c)
}

func (g *Generator) getTCPProxyConfigImpl(spec *ytv1.TCPProxiesSpec) (TCPProxyServer, error) {
	c, err := getTCPProxyServerCarcass(spec)
	if err != nil {
		return TCPProxyServer{}, err
	}

	g.fillCommonService(&c.CommonServer, &spec.InstanceSpec)

	return c, nil
}

func (g *Generator) GetTCPProxyConfig(spec ytv1.TCPProxiesSpec) ([]byte, error) {
	if g.ytsaurus.Spec.TCPProxies == nil {
		return []byte{}, nil
	}

	c, err := g.getTCPProxyConfigImpl(&spec)
	if err != nil {
		return []byte{}, err
	}

	return marshallYSONConfig(c)
}

func (g *Generator) getControllerAgentConfigImpl() (ControllerAgentServer, error) {
	spec := g.ytsaurus.Spec.ControllerAgents
	c, err := getControllerAgentServerCarcass(spec)
	if err != nil {
		return ControllerAgentServer{}, err
	}

	c.ControllerAgent.EnableTmpfs = g.ytsaurus.Spec.UsePorto
	c.ControllerAgent.UseColumnarStatisticsDefault = true
	c.ControllerAgent.EnableSnapshotLoading = true

	g.fillCommonService(&c.CommonServer, &spec.InstanceSpec)
	g.fillBusServer(&c.CommonServer, spec.NativeTransport)

	return c, nil
}

func (g *Generator) GetControllerAgentConfig() ([]byte, error) {
	if g.ytsaurus.Spec.ControllerAgents == nil {
		return []byte{}, nil
	}

	c, err := g.getControllerAgentConfigImpl()
	if err != nil {
		return []byte{}, err
	}

	return marshallYSONConfig(c)
}

func (g *Generator) getDataNodeConfigImpl(spec *ytv1.DataNodesSpec) (DataNodeServer, error) {
	c, err := getDataNodeServerCarcass(spec)
	if err != nil {
		return DataNodeServer{}, err
	}

	g.fillCommonService(&c.CommonServer, &spec.InstanceSpec)
	g.fillBusServer(&c.CommonServer, spec.NativeTransport)
	return c, nil
}

func (g *Generator) GetDataNodeConfig(spec ytv1.DataNodesSpec) ([]byte, error) {
	c, err := g.getDataNodeConfigImpl(&spec)
	if err != nil {
		return []byte{}, err
	}
	return marshallYSONConfig(c)
}

func (g *Generator) getExecNodeConfigImpl(spec *ytv1.ExecNodesSpec) (ExecNodeServer, error) {
	c, err := getExecNodeServerCarcass(
		spec,
		g.ytsaurus.Spec.UsePorto)
	if err != nil {
		return c, err
	}
	g.fillCommonService(&c.CommonServer, &spec.InstanceSpec)
	g.fillBusServer(&c.CommonServer, spec.NativeTransport)
	return c, nil
}

func (g *Generator) GetExecNodeConfig(spec ytv1.ExecNodesSpec) ([]byte, error) {
	c, err := g.getExecNodeConfigImpl(&spec)
	if err != nil {
		return []byte{}, err
	}
	return marshallYSONConfig(c)
}

func (g *Generator) getTabletNodeConfigImpl(spec *ytv1.TabletNodesSpec) (TabletNodeServer, error) {
	c, err := getTabletNodeServerCarcass(spec)
	if err != nil {
		return c, err
	}
	g.fillCommonService(&c.CommonServer, &spec.InstanceSpec)
	g.fillBusServer(&c.CommonServer, spec.NativeTransport)
	return c, nil
}

func (g *Generator) GetTabletNodeConfig(spec ytv1.TabletNodesSpec) ([]byte, error) {
	c, err := g.getTabletNodeConfigImpl(&spec)
	if err != nil {
		return nil, err
	}
	return marshallYSONConfig(c)
}

func (g *Generator) getHTTPProxyConfigImpl(spec *ytv1.HTTPProxiesSpec) (HTTPProxyServer, error) {
	c, err := getHTTPProxyServerCarcass(spec)
	if err != nil {
		return c, err
	}

	g.fillDriver(&c.Driver)
	g.fillCommonService(&c.CommonServer, &spec.InstanceSpec)
	g.fillBusServer(&c.CommonServer, spec.NativeTransport)
	if spec.Transport.HTTPPort != nil {
		c.Port = int(*spec.Transport.HTTPPort)
	}

	if g.ytsaurus.Spec.OauthService != nil {
		c.Auth.OauthService = &OauthService{
			Host:               g.ytsaurus.Spec.OauthService.Host,
			Port:               g.ytsaurus.Spec.OauthService.Port,
			Secure:             g.ytsaurus.Spec.OauthService.Secure,
			UserInfoEndpoint:   g.ytsaurus.Spec.OauthService.UserInfo.Endpoint,
			UserInfoLoginField: g.ytsaurus.Spec.OauthService.UserInfo.LoginField,
			UserInfoErrorField: g.ytsaurus.Spec.OauthService.UserInfo.ErrorField,
		}
		c.Auth.OauthCookieAuthenticator = &OauthCookieAuthenticator{}
		c.Auth.OauthTokenAuthenticator = &OauthTokenAuthenticator{}
	}

	return c, nil
}

func (g *Generator) GetHTTPProxyConfig(spec ytv1.HTTPProxiesSpec) ([]byte, error) {
	c, err := g.getHTTPProxyConfigImpl(&spec)
	if err != nil {
		return nil, err
	}

	return marshallYSONConfig(c)
}

func (g *Generator) getQueryTrackerConfigImpl() (QueryTrackerServer, error) {
	spec := g.ytsaurus.Spec.QueryTrackers
	c, err := getQueryTrackerServerCarcass(spec)
	if err != nil {
		return c, err
	}
	g.fillCommonService(&c.CommonServer, &spec.InstanceSpec)
	g.fillBusServer(&c.CommonServer, spec.NativeTransport)

	return c, nil
}

func (g *Generator) GetQueryTrackerConfig() ([]byte, error) {
	if g.ytsaurus.Spec.QueryTrackers == nil {
		return []byte{}, nil
	}

	c, err := g.getQueryTrackerConfigImpl()
	if err != nil {
		return nil, err
	}

	return marshallYSONConfig(c)
}

func (g *Generator) getQueueAgentConfigImpl() (QueueAgentServer, error) {
	spec := g.ytsaurus.Spec.QueueAgents
	c, err := getQueueAgentServerCarcass(spec)
	if err != nil {
		return c, err
	}

	g.fillCommonService(&c.CommonServer, &spec.InstanceSpec)
	g.fillBusServer(&c.CommonServer, spec.NativeTransport)

	return c, nil
}

func (g *Generator) GetQueueAgentConfig() ([]byte, error) {
	if g.ytsaurus.Spec.QueueAgents == nil {
		return []byte{}, nil
	}

	c, err := g.getQueueAgentConfigImpl()
	if err != nil {
		return nil, err
	}

	return marshallYSONConfig(c)
}

func (g *Generator) getYQLAgentConfigImpl() (YQLAgentServer, error) {
	spec := g.ytsaurus.Spec.YQLAgents
	c, err := getYQLAgentServerCarcass(spec)
	if err != nil {
		return c, err
	}
	g.fillCommonService(&c.CommonServer, &spec.InstanceSpec)
	g.fillBusServer(&c.CommonServer, spec.NativeTransport)

	c.YQLAgent.GatewayConfig.ClusterMapping = []ClusterMapping{
		{
			Name:    g.ytsaurus.Name,
			Cluster: g.GetHTTPProxiesServiceAddress(consts.DefaultHTTPProxyRole),
			Default: true,
		},
	}

	// For backward compatibility.
	c.YQLAgent.AdditionalClusters = map[string]string{
		g.ytsaurus.Name: g.GetHTTPProxiesServiceAddress(consts.DefaultHTTPProxyRole),
	}
	c.YQLAgent.DefaultCluster = g.ytsaurus.Name

	return c, nil
}

func (g *Generator) GetYQLAgentConfig() ([]byte, error) {
	if g.ytsaurus.Spec.YQLAgents == nil {
		return []byte{}, nil
	}
	c, err := g.getYQLAgentConfigImpl()
	if err != nil {
		return nil, err
	}
	return marshallYSONConfig(c)
}

func (g *Generator) GetUIClustersConfig() ([]byte, error) {
	if g.ytsaurus.Spec.UI == nil {
		return []byte{}, nil
	}

	c := getUIClusterCarcass()
	c.ID = g.ytsaurus.Name
	c.Name = g.ytsaurus.Name
	c.Proxy = g.GetHTTPProxiesAddress(consts.DefaultHTTPProxyRole)
	c.PrimaryMaster.CellTag = g.ytsaurus.Spec.PrimaryMasters.CellTag

	if port := g.ytsaurus.Spec.UI.ProxyPort; port != nil {
		c.ProxyPort = *port
	}

	c.Theme = g.ytsaurus.Spec.UI.Theme
	c.Environment = g.ytsaurus.Spec.UI.Environment
	if g.ytsaurus.Spec.UI.Group != nil {
		c.Group = *g.ytsaurus.Spec.UI.Group
	}
	if g.ytsaurus.Spec.UI.Description != nil {
		c.Description = *g.ytsaurus.Spec.UI.Description
	}

	return marshallYSONConfig(UIClusters{
		Clusters: []UICluster{c},
	})
}

func (g *Generator) GetUICustomConfig() ([]byte, error) {
	if g.ytsaurus.Spec.UI == nil {
		return []byte{}, nil
	}

	c := UICustom{
		OdinBaseURL: g.ytsaurus.Spec.UI.OdinBaseUrl,
		UISettings: &UISettings{
			DirectDownload: ptr.Bool(false),
		},
	}

	return marshallYSONConfig(c)
}

func (g *Generator) getDiscoveryConfigImpl() (DiscoveryServer, error) {
	spec := &g.ytsaurus.Spec.Discovery
	c, err := getDiscoveryServerCarcass(spec)
	if err != nil {
		return c, err
	}

	g.fillCommonService(&c.CommonServer, &spec.InstanceSpec)
	g.fillBusServer(&c.CommonServer, spec.NativeTransport)
	c.DiscoveryServer.Addresses = g.getDiscoveryAddresses()
	return c, nil
}

func (g *Generator) GetDiscoveryConfig() ([]byte, error) {
	c, err := g.getDiscoveryConfigImpl()
	if err != nil {
		return nil, err
	}
	return marshallYSONConfig(c)
}
