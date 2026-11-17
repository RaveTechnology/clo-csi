package controller

import (
	"context"
	"encoding/json"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"math"
	"strings"
	"time"
)

type Service interface {
	Ready(context.Context) bool
	EnsureVolume(context.Context, protocol.VolumeRequest) (registry.VolumeRecord, error)
	LookupVolume(context.Context, string) (registry.VolumeRecord, error)
	Delete(context.Context, string) error
	Publish(context.Context, string, string, bool) (protocol.PublishContext, error)
	Unpublish(context.Context, string, string) error
	Expand(context.Context, string, int64, int64) (int64, error)
}
type Server struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedControllerServer
	service Service
	version string
}

func New(service Service, version string) *Server {
	if version == "" {
		version = "dev"
	}
	return &Server{service: service, version: version}
}
func validHandle(handle string) bool {
	parts := strings.Split(handle, ":")
	if len(parts) != 3 || parts[0] != "clo" {
		return false
	}
	for _, p := range parts[1:] {
		if _, e := uuid.Parse(p); e != nil {
			return false
		}
	}
	return true
}
func validNode(id string) bool {
	p := strings.Split(id, ":")
	if len(p) != 4 || p[0] != "clo-node" {
		return false
	}
	for _, v := range p[1:] {
		if _, e := uuid.Parse(v); e != nil {
			return false
		}
	}
	return true
}
func capabilities(caps []*csi.VolumeCapability) (string, error) {
	if len(caps) == 0 {
		return "", status.Error(codes.InvalidArgument, "volume capabilities required")
	}
	for _, cap := range caps {
		if cap == nil || cap.GetMount() == nil || cap.GetAccessMode() == nil || cap.GetAccessMode().Mode != csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER {
			return "", status.Error(codes.InvalidArgument, "only RWO Filesystem is supported")
		}
		m := cap.GetMount()
		if m.FsType != "" && m.FsType != "ext4" || len(m.MountFlags) > 0 || m.VolumeMountGroup != "" {
			return "", status.Error(codes.InvalidArgument, "only ext4 without custom mount options is supported")
		}
	}
	return "ext4", nil
}
func capacityRange(r *csi.CapacityRange) (int64, int64, error) {
	if r == nil {
		return 0, 0, nil
	}
	if r.RequiredBytes < 0 || r.LimitBytes < 0 || r.LimitBytes > 0 && r.RequiredBytes > r.LimitBytes {
		return 0, 0, status.Error(codes.InvalidArgument, "invalid capacity range")
	}
	return r.RequiredBytes, r.LimitBytes, nil
}
func (s *Server) operation(ctx context.Context) (context.Context, context.CancelFunc, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	if !s.service.Ready(ctx) {
		cancel()
		return ctx, func() {}, status.Error(codes.Unavailable, "CSI mutation readiness pending")
	}
	return ctx, cancel, nil
}
func (s *Server) CreateVolume(ctx context.Context, q *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if q.GetName() == "" || len(q.GetParameters()) != 0 || q.GetVolumeContentSource() != nil || q.GetAccessibilityRequirements() != nil {
		return nil, status.Error(codes.InvalidArgument, "named new volumes without parameters/content/topology required")
	}
	fs, e := capabilities(q.GetVolumeCapabilities())
	if e != nil {
		return nil, e
	}
	required, limit, e := capacityRange(q.GetCapacityRange())
	if e != nil {
		return nil, e
	}
	ctx, cancel, e := s.operation(ctx)
	if e != nil {
		return nil, e
	}
	defer cancel()
	v, e := s.service.EnsureVolume(ctx, protocol.VolumeRequest{Name: q.Name, RequiredBytes: required, LimitBytes: limit, FSType: fs, AccessMode: "SINGLE_NODE_WRITER"})
	if e != nil {
		return nil, rpcError(e)
	}
	handle := "clo:" + v.Spec.InstallationID + ":" + v.Status.ProviderID
	if !validHandle(handle) || v.Status.SizeGB <= 0 || v.Status.SizeGB > int(math.MaxInt64/(1<<30)) {
		return nil, status.Error(codes.Internal, "invalid provisioned volume result")
	}
	return &csi.CreateVolumeResponse{Volume: &csi.Volume{VolumeId: handle, CapacityBytes: int64(v.Status.SizeGB) << 30}}, nil
}
func (s *Server) DeleteVolume(ctx context.Context, q *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if q.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume handle required")
	}
	if !validHandle(q.GetVolumeId()) {
		// Such an ID cannot name a volume issued by this driver. CSI requires
		// idempotent success for absent volumes; never forward it to cloud code.
		return &csi.DeleteVolumeResponse{}, nil
	}
	ctx, cancel, e := s.operation(ctx)
	if e != nil {
		return nil, e
	}
	defer cancel()
	if e = s.service.Delete(ctx, q.VolumeId); e != nil {
		return nil, rpcError(e)
	}
	return &csi.DeleteVolumeResponse{}, nil
}
func (s *Server) ControllerPublishVolume(ctx context.Context, q *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	if !validHandle(q.GetVolumeId()) || !validNode(q.GetNodeId()) || q.GetReadonly() {
		return nil, status.Error(codes.InvalidArgument, "valid volume/node IDs and writable controller attachment required")
	}
	if _, e := capabilities([]*csi.VolumeCapability{q.GetVolumeCapability()}); e != nil {
		return nil, e
	}
	ctx, cancel, e := s.operation(ctx)
	if e != nil {
		return nil, e
	}
	defer cancel()
	out, e := s.service.Publish(ctx, q.VolumeId, q.NodeId, false)
	if e != nil {
		return nil, rpcError(e)
	}
	raw, e := json.Marshal(out)
	if e != nil {
		return nil, status.Error(codes.Internal, "publish context encoding failed")
	}
	return &csi.ControllerPublishVolumeResponse{PublishContext: map[string]string{"clo-context": string(raw)}}, nil
}
func (s *Server) ControllerUnpublishVolume(ctx context.Context, q *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	if !validHandle(q.GetVolumeId()) || q.GetNodeId() != "" && !validNode(q.NodeId) {
		return nil, status.Error(codes.InvalidArgument, "invalid volume/node ID")
	}
	ctx, cancel, e := s.operation(ctx)
	if e != nil {
		return nil, e
	}
	defer cancel()
	if e = s.service.Unpublish(ctx, q.VolumeId, q.NodeId); e != nil {
		return nil, rpcError(e)
	}
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}
func (s *Server) ControllerExpandVolume(ctx context.Context, q *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	if !validHandle(q.GetVolumeId()) {
		return nil, status.Error(codes.InvalidArgument, "invalid volume handle")
	}
	required, limit, e := capacityRange(q.GetCapacityRange())
	if e != nil {
		return nil, e
	}
	if required <= 0 {
		return nil, status.Error(codes.InvalidArgument, "positive expansion size required")
	}
	if q.GetVolumeCapability() != nil {
		if _, e = capabilities([]*csi.VolumeCapability{q.VolumeCapability}); e != nil {
			return nil, e
		}
	}
	ctx, cancel, e := s.operation(ctx)
	if e != nil {
		return nil, e
	}
	defer cancel()
	size, e := s.service.Expand(ctx, q.VolumeId, required, limit)
	if e != nil {
		return nil, rpcError(e)
	}
	if size < required || limit > 0 && size > limit {
		return nil, status.Error(codes.Internal, "invalid expansion result")
	}
	return &csi.ControllerExpandVolumeResponse{CapacityBytes: size, NodeExpansionRequired: true}, nil
}
func (s *Server) ValidateVolumeCapabilities(ctx context.Context, q *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if !validHandle(q.GetVolumeId()) || len(q.GetVolumeCapabilities()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "valid handle and capabilities required")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if _, e := s.service.LookupVolume(ctx, q.VolumeId); e != nil {
		return nil, rpcError(e)
	}
	if _, e := capabilities(q.VolumeCapabilities); e != nil {
		return &csi.ValidateVolumeCapabilitiesResponse{Message: "only RWO/ext4 Filesystem is supported"}, nil
	}
	if len(q.Parameters) != 0 {
		return &csi.ValidateVolumeCapabilitiesResponse{Message: "custom parameters are unsupported"}, nil
	}
	return &csi.ValidateVolumeCapabilitiesResponse{Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{VolumeCapabilities: q.VolumeCapabilities, VolumeContext: q.VolumeContext}}, nil
}
func (s *Server) ControllerGetCapabilities(context.Context, *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	var caps []*csi.ControllerServiceCapability
	for _, kind := range []csi.ControllerServiceCapability_RPC_Type{csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME, csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME, csi.ControllerServiceCapability_RPC_EXPAND_VOLUME} {
		caps = append(caps, &csi.ControllerServiceCapability{Type: &csi.ControllerServiceCapability_Rpc{Rpc: &csi.ControllerServiceCapability_RPC{Type: kind}}})
	}
	return &csi.ControllerGetCapabilitiesResponse{Capabilities: caps}, nil
}
