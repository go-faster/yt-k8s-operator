package ytconfig

type UIAuthenticationType string

const (
	uiAuthenticationBasic UIAuthenticationType = "basic"
)

type UIPrimaryMaster struct {
	CellTag int16 `yson:"cellTag"`
}

type UICluster struct {
	ID             string               `yson:"id"`
	Name           string               `yson:"name"`
	Proxy          string               `yson:"proxy"`
	ProxyPort      int                  `yson:"proxyPort,omitempty"`
	Secure         bool                 `yson:"secure"`
	Authentication UIAuthenticationType `yson:"authentication"`
	Group          string               `yson:"group"`
	Theme          string               `yson:"theme"`
	Environment    string               `yson:"environment"`
	Description    string               `yson:"description"`
	PrimaryMaster  UIPrimaryMaster      `yson:"primaryMaster"`
}

type UIClusters struct {
	Clusters []UICluster `yson:"clusters"`
}

func getUIClusterCarcass() UICluster {
	return UICluster{
		Secure:         false,
		Authentication: uiAuthenticationBasic,
		Group:          "My YTsaurus clusters",
		Description:    "My first YTsaurus. Handle with care.",
	}
}

// UISettings represents the subset of UISettings from the typescript definition.
//
// See packages/ui/src/shared/ui-settings.ts.
type UISettings struct {
	DirectDownload *bool `yson:"directDownload,omitempty"`
}

type UICustom struct {
	OdinBaseURL *string     `yson:"odinBaseUrl,omitempty"`
	UISettings  *UISettings `yson:"uiSettings,omitempty"`
}
