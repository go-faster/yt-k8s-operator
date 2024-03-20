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
	ytsaurus *apiproxy.Ytsaurus
	yt       YtsaurusClient
	labeller labeller.Labeller
	nodePort string
}

func newRackSetup(
	ytsaurus *apiproxy.Ytsaurus,
	yt YtsaurusClient,
	labeller labeller.Labeller,
	nodePort int,
) *rackSetup {
	return &rackSetup{
		ytsaurus: ytsaurus,
		yt:       yt,
		labeller: labeller,
		nodePort: strconv.Itoa(nodePort),
	}
}

func (r *rackSetup) SetRacks(ctx context.Context) error {
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

	var (
		// rack name -> [host]
		racks = map[string]map[string]struct{}{}
		// dc name -> [rack]
		dcs = map[string]map[string]struct{}{}
	)

	for _, pod := range podList.Items {
		rack, ok := pod.Labels[spec.RackLabel]
		if !ok {
			continue
		}
		host := net.JoinHostPort(pod.Spec.Hostname, r.nodePort)

		hosts, ok := racks[rack]
		if !ok {
			hosts = map[string]struct{}{}
			racks[rack] = hosts
		}
		hosts[host] = struct{}{}

		if dc, ok := pod.Labels[spec.DCLabel]; ok {
			dcracks, ok := dcs[dc]
			if !ok {
				dcracks = map[string]struct{}{}
				dcs[dc] = dcracks
			}
			dcracks[rack] = struct{}{}
		}
	}

	for rack, hosts := range racks {
		if err := r.ensureRack(ctx, rack); err != nil {
			log.Error(err, "Ensure rack",
				"rack", rack,
			)
			continue
		}
		for host := range hosts {
			if err := r.setHostRack(ctx, host, rack); err != nil {
				log.Error(err, "Set host's rack",
					"host", host,
					"rack", rack,
				)
				continue
			}
		}
	}

	for dc, dcracks := range dcs {
		if err := r.ensureDC(ctx, dc); err != nil {
			log.Error(err, "Ensure DC",
				"dc", dc,
			)
			continue
		}
		for rack := range dcracks {
			if err := r.setRackDC(ctx, rack, dc); err != nil {
				log.Error(err, "Set rack's datacenter",
					"rack", rack,
					"dc", dc,
				)
				continue
			}

			hosts := racks[rack]
			for host := range hosts {
				if err := r.setHostDC(ctx, host, dc); err != nil {
					log.Error(err, "Set host's datacenter",
						"host", host,
						"dc", dc,
					)
					continue
				}
			}
		}
	}

	return nil
}

func (r *rackSetup) ensureRack(ctx context.Context, rack string) error {
	log := klog.FromContext(ctx)
	yc := r.yt.GetYtClient()

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

func (r *rackSetup) setHostRack(ctx context.Context, host, rack string) error {
	yc := r.yt.GetYtClient()
	// Same as
	//
	// 	yt set "//sys/cluster_nodes/$host/@rack" "$rack"
	//
	if err := yc.SetNode(ctx,
		ypath.Path("//sys/cluster_nodes").Child(host).Attr("rack"),
		rack,
		&yt.SetNodeOptions{},
	); err != nil {
		return err
	}
	return nil
}

func (r *rackSetup) ensureDC(ctx context.Context, dc string) error {
	log := klog.FromContext(ctx)
	yc := r.yt.GetYtClient()

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

func (r *rackSetup) setRackDC(ctx context.Context, rack, dc string) error {
	yc := r.yt.GetYtClient()
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

func (r *rackSetup) setHostDC(ctx context.Context, host, dc string) error {
	yc := r.yt.GetYtClient()
	// Same as
	//
	// 	yt set "//sys/cluster_nodes/$host/@data_center" "$dc"
	//
	if err := yc.SetNode(ctx,
		ypath.Path("//sys/cluster_nodes").Child(host).Attr("data_center"),
		dc,
		&yt.SetNodeOptions{},
	); err != nil {
		return err
	}
	return nil
}
