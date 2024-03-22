package components

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"github.com/ytsaurus/yt-k8s-operator/pkg/apiproxy"
	"github.com/ytsaurus/yt-k8s-operator/pkg/labeller"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
	corev1 "k8s.io/api/core/v1"
	klog "sigs.k8s.io/controller-runtime/pkg/log"
)

type rackSetup struct {
	serviceName func(name string) string
	ytsaurus    *apiproxy.Ytsaurus
	labeller    labeller.Labeller
	nodePort    string
}

func newRackSetup(
	serviceName func(name string) string,
	ytsaurus *apiproxy.Ytsaurus,
	labeller labeller.Labeller,
	nodePort int,
) *rackSetup {
	return &rackSetup{
		serviceName: serviceName,
		ytsaurus:    ytsaurus,
		labeller:    labeller,
		nodePort:    strconv.Itoa(nodePort),
	}
}

func (r *rackSetup) SetRacks(ctx context.Context, yc yt.Client) error {
	log := klog.FromContext(ctx)

	spec := r.ytsaurus.GetResource().Spec.RackAwareness
	if !spec.Enable {
		return nil
	}
	log.Info("Creating racks",
		"rack_label", spec.RackLabel,
		"dc_label", spec.DCLabel,
	)

	podList := &corev1.PodList{}
	if err := r.ytsaurus.APIProxy().ListObjects(ctx, podList, r.labeller.GetListOptions()...); err != nil {
		return fmt.Errorf("query pods: %w", err)
	}

	type mapping = map[string]map[string]struct{}
	var (
		set = func(m mapping, key, value string) {
			values, ok := m[key]
			if !ok {
				values = map[string]struct{}{}
				m[key] = values
			}
			values[value] = struct{}{}
		}

		// rack name -> [cluster node]
		racksNodes = mapping{}
		// rack name -> [host]
		racks = mapping{}
		// dc name -> [rack]
		dcs = mapping{}
	)

	for _, pod := range podList.Items {
		rack, ok := pod.Labels[spec.RackLabel]
		if !ok {
			continue
		}

		if host := pod.Spec.NodeName; host != "" {
			set(racks, rack, host)
		}

		node := net.JoinHostPort(r.serviceName(pod.Spec.Hostname), r.nodePort)
		set(racksNodes, rack, node)

		if dc, ok := pod.Labels[spec.DCLabel]; ok {
			set(dcs, dc, rack)
		}
	}

	for rack, hosts := range racks {
		if err := r.ensureRack(ctx, yc, rack); err != nil {
			log.Error(err, "Ensure rack",
				"rack", rack,
			)
			continue
		}

		for host := range hosts {
			if err := r.setHostRack(ctx, yc, host, rack); err != nil {
				log.Error(err, "Set host's rack",
					"host", host,
					"rack", rack,
				)
				continue
			}
		}

		nodes := racksNodes[rack]
		for node := range nodes {
			if err := r.setNodeRack(ctx, yc, node, rack); err != nil {
				log.Error(err, "Set node's rack",
					"node", node,
					"rack", rack,
				)
				continue
			}
		}
	}

	for dc, dcracks := range dcs {
		if err := r.ensureDC(ctx, yc, dc); err != nil {
			log.Error(err, "Ensure DC",
				"dc", dc,
			)
			continue
		}
		for rack := range dcracks {
			if err := r.setRackDC(ctx, yc, rack, dc); err != nil {
				log.Error(err, "Set rack's datacenter",
					"rack", rack,
					"dc", dc,
				)
				continue
			}

			hosts := racks[rack]
			for host := range hosts {
				if err := r.setHostDC(ctx, yc, host, dc); err != nil {
					log.Error(err, "Set host's datacenter",
						"host", host,
						"dc", dc,
					)
					continue
				}
			}

			nodes := racks[rack]
			for node := range nodes {
				if err := r.setNodeDC(ctx, yc, node, dc); err != nil {
					log.Error(err, "Set node's datacenter",
						"node", node,
						"dc", dc,
					)
					continue
				}
			}
		}
	}

	return nil
}

func (r *rackSetup) ensureRack(ctx context.Context, yc yt.Client, rack string) error {
	log := klog.FromContext(ctx)

	exist, err := yc.NodeExists(ctx, ypath.Path("//sys/racks").Child(rack), &yt.NodeExistsOptions{})
	if err != nil {
		return err
	}

	if exist {
		log.V(1).Info("Rack already exist", "rack", rack)
		return nil
	}
	log.V(1).Info("Creating rack", "rack", rack)

	// Same as
	//
	// 	yt create rack --attributes "{name=$rack}"
	//
	if _, err = yc.CreateObject(ctx, yt.NodeType("rack"), &yt.CreateObjectOptions{
		Attributes: map[string]any{
			"name": rack,
		},
	}); err != nil && !yterrors.ContainsAlreadyExistsError(err) {
		return fmt.Errorf("create rack %q: %w", rack, err)
	}

	return nil
}

func (r *rackSetup) setHostRack(ctx context.Context, yc yt.Client, host, rack string) error {
	// Same as
	//
	// 	yt set "//sys/hosts/$host/@rack" "$rack"
	//
	if err := yc.SetNode(ctx,
		ypath.Path("//sys/hosts").Child(host).Attr("rack"),
		rack,
		&yt.SetNodeOptions{},
	); err != nil {
		return err
	}
	return nil
}

func (r *rackSetup) setNodeRack(ctx context.Context, yc yt.Client, node, rack string) error {
	// Same as
	//
	// 	yt set "//sys/cluster_nodes/$node/@rack" "$rack"
	//
	if err := yc.SetNode(ctx,
		ypath.Path("//sys/cluster_nodes").Child(node).Attr("rack"),
		rack,
		&yt.SetNodeOptions{},
	); err != nil {
		return err
	}
	return nil
}

func (r *rackSetup) ensureDC(ctx context.Context, yc yt.Client, dc string) error {
	log := klog.FromContext(ctx)

	exist, err := yc.NodeExists(ctx, ypath.Path("//sys/data_centers").Child(dc), &yt.NodeExistsOptions{})
	if err != nil {
		return err
	}

	if exist {
		log.V(1).Info("Data center already exist", "dc", dc)
		return nil
	}
	log.V(1).Info("Creating data center", "dc", dc)

	// Same as
	//
	// 	yt create data_center --attributes "{name=$dc}"
	//
	if _, err = yc.CreateObject(ctx, yt.NodeType("data_center"), &yt.CreateObjectOptions{
		Attributes: map[string]any{
			"name": dc,
		},
	}); err != nil && !yterrors.ContainsAlreadyExistsError(err) {
		return fmt.Errorf("create data center %q: %w", dc, err)
	}

	return nil
}

func (r *rackSetup) setRackDC(ctx context.Context, yc yt.Client, rack, dc string) error {
	// Same as
	//
	// 	yt set "//sys/racks/$rack/@data_center" "$dc"
	//
	if err := yc.SetNode(ctx,
		ypath.Path("//sys/racks").Child(rack).Attr("data_center"),
		dc,
		&yt.SetNodeOptions{},
	); err != nil {
		return err
	}
	return nil
}

func (r *rackSetup) setHostDC(ctx context.Context, yc yt.Client, host, dc string) error {
	// Same as
	//
	// 	yt set "//sys/hosts/$host/@data_center" "$dc"
	//
	if err := yc.SetNode(ctx,
		ypath.Path("//sys/hosts").Child(host).Attr("data_center"),
		dc,
		&yt.SetNodeOptions{},
	); err != nil {
		return err
	}
	return nil
}

func (r *rackSetup) setNodeDC(ctx context.Context, yc yt.Client, node, dc string) error {
	// Same as
	//
	// 	yt set "//sys/cluster_nodes/$node/@data_center" "$dc"
	//
	if err := yc.SetNode(ctx,
		ypath.Path("//sys/cluster_nodes").Child(node).Attr("data_center"),
		dc,
		&yt.SetNodeOptions{},
	); err != nil {
		return err
	}
	return nil
}
