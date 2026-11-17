package node

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"io"
	"strings"
	"time"
)

type NodeConfig struct {
	KubeletRoot, Version string
	MaxVolumesPerNode    int
}
type Server struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedNodeServer
	runtime VolumeRuntime
	auth    protocol.Authorizer
	cfg     NodeConfig
}

func NewServer(runtime VolumeRuntime, auth protocol.Authorizer, cfg NodeConfig) *Server {
	if cfg.Version == "" {
		cfg.Version = "dev"
	}
	if cfg.MaxVolumesPerNode <= 0 {
		cfg.MaxVolumesPerNode = 8
	}
	return &Server{runtime: runtime, auth: auth, cfg: cfg}
}
func nodeError(err error) error {
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, "request canceled")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, "request deadline exceeded")
	}
	var e *protocol.Error
	if !errors.As(err, &e) {
		return status.Error(codes.Internal, "node filesystem operation failed")
	}
	code := codes.Internal
	switch e.Code {
	case protocol.Invalid:
		code = codes.InvalidArgument
	case protocol.NotFound:
		code = codes.NotFound
	case protocol.Exists:
		code = codes.AlreadyExists
	case protocol.Conflict:
		code = codes.Aborted
	case protocol.Blocked, protocol.RecoveryRequired:
		code = codes.FailedPrecondition
	case protocol.Unavailable:
		code = codes.Unavailable
	}
	return status.Error(code, e.Message)
}
func validVolumeHandle(handle string) bool {
	parts := strings.Split(handle, ":")
	if len(parts) != 3 || parts[0] != "clo" {
		return false
	}
	for _, v := range parts[1:] {
		if _, e := uuid.Parse(v); e != nil {
			return false
		}
	}
	return true
}
func nodeCapability(cap *csi.VolumeCapability) error {
	if cap == nil || cap.GetMount() == nil || cap.GetAccessMode() == nil || cap.GetAccessMode().Mode != csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER {
		return status.Error(codes.InvalidArgument, "only RWO Filesystem is supported")
	}
	mount := cap.GetMount()
	if mount.FsType != "" && mount.FsType != "ext4" || len(mount.MountFlags) > 0 || mount.VolumeMountGroup != "" {
		return status.Error(codes.InvalidArgument, "only ext4 without custom mount options is supported")
	}
	return nil
}
func contextForVolume(handle string, values map[string]string) (protocol.PublishContext, error) {
	var out protocol.PublishContext
	if !validVolumeHandle(handle) || len(values) != 1 || values["clo-context"] == "" || len(values["clo-context"]) > 1<<20 {
		return out, protocol.Fail(protocol.Invalid, "valid volume handle and publish context required")
	}
	d := json.NewDecoder(strings.NewReader(values["clo-context"]))
	d.DisallowUnknownFields()
	if e := d.Decode(&out); e != nil {
		return out, protocol.Fail(protocol.Invalid, "invalid publish context")
	}
	var extra any
	if d.Decode(&extra) != io.EOF || out.Volume.Handle != handle || out.Volume.RecordID == "" || out.Volume.Serial == "" || out.Volume.AttachEpoch < 1 || out.Volume.FSUUID != out.Volume.ProviderID || out.Node.BindingID == "" || out.Node.Epoch < 1 {
		return out, protocol.Fail(protocol.Invalid, "publish context identities differ")
	}
	parts := strings.Split(handle, ":")
	if parts[2] != out.Volume.ProviderID {
		return out, protocol.Fail(protocol.Invalid, "publish provider ID differs")
	}
	return out, nil
}
func (s *Server) permit(ctx context.Context, p protocol.PublishContext, format bool) (protocol.StagePermit, error) {
	node, e := s.auth.Binding(ctx)
	if e != nil {
		return protocol.StagePermit{}, e
	}
	if node != p.Node {
		return protocol.StagePermit{}, protocol.Fail(protocol.Blocked, "authenticated node generation differs")
	}
	snap, e := s.runtime.Snapshot(ctx)
	if e != nil {
		return protocol.StagePermit{}, e
	}
	permit, e := s.auth.AuthorizeStage(ctx, protocol.StageRequest{Volume: p.Volume, Node: node, Snapshot: snap, Format: format})
	if e != nil {
		return permit, e
	}
	if permit.Node != node || permit.Volume != p.Volume {
		return permit, protocol.Fail(protocol.Blocked, "controller permit identity differs")
	}
	return permit, nil
}
func (s *Server) NodeStageVolume(ctx context.Context, q *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	if e := nodeCapability(q.GetVolumeCapability()); e != nil {
		return nil, e
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	if e := (Paths{Root: s.cfg.KubeletRoot}).Check(q.GetStagingTargetPath(), "stage", false); e != nil {
		return nil, nodeError(e)
	}
	p, e := contextForVolume(q.GetVolumeId(), q.GetPublishContext())
	if e != nil {
		return nil, nodeError(e)
	}
	permit, e := s.permit(ctx, p, false)
	if e != nil {
		return nil, nodeError(e)
	}
	result, e := s.runtime.Stage(ctx, permit, q.StagingTargetPath)
	if errors.Is(e, ErrNeedFormat) {
		permit, e = s.permit(ctx, p, true)
		if e == nil {
			result, e = s.runtime.Stage(ctx, permit, q.StagingTargetPath)
		}
	}
	if e != nil {
		return nil, nodeError(e)
	}
	if result.NeedsReport {
		if !result.FilesystemChecked || permit.OperationID == "" || permit.Nonce == "" {
			return nil, status.Error(codes.FailedPrecondition, "filesystem proof or format operation identity absent")
		}
		receipt := protocol.Receipt{JobID: permit.OperationID, Nonce: permit.Nonce, Type: "formatComplete", Node: permit.Node, Volume: permit.Volume, Snapshot: result.Snapshot, FilesystemChecked: true, ObservedAt: time.Now().UTC()}
		if e = s.auth.Report(ctx, receipt); e != nil {
			return nil, nodeError(e)
		}
		permit, e = s.permit(ctx, p, false)
		if e != nil {
			return nil, nodeError(e)
		}
		result, e = s.runtime.Stage(ctx, permit, q.StagingTargetPath)
		if e != nil {
			return nil, nodeError(e)
		}
		if result.NeedsReport {
			return nil, status.Error(codes.FailedPrecondition, "controller filesystem initialization remains pending")
		}
	}
	return &csi.NodeStageVolumeResponse{}, nil
}
func (s *Server) NodePublishVolume(ctx context.Context, q *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if e := nodeCapability(q.GetVolumeCapability()); e != nil {
		return nil, e
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	paths := Paths{Root: s.cfg.KubeletRoot}
	if e := paths.Check(q.GetStagingTargetPath(), "stage", false); e != nil {
		return nil, nodeError(e)
	}
	if e := paths.Check(q.GetTargetPath(), "target", false); e != nil {
		return nil, nodeError(e)
	}
	p, e := contextForVolume(q.GetVolumeId(), q.GetPublishContext())
	if e != nil {
		return nil, nodeError(e)
	}
	permit, e := s.permit(ctx, p, false)
	if e != nil {
		return nil, nodeError(e)
	}
	if e = s.runtime.Publish(ctx, permit, q.StagingTargetPath, q.TargetPath, q.Readonly); e != nil {
		return nil, nodeError(e)
	}
	return &csi.NodePublishVolumeResponse{}, nil
}
func (s *Server) NodeUnpublishVolume(ctx context.Context, q *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if !validVolumeHandle(q.GetVolumeId()) {
		return nil, status.Error(codes.InvalidArgument, "invalid volume handle")
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if e := s.runtime.Unpublish(ctx, q.VolumeId, q.GetTargetPath()); e != nil {
		return nil, nodeError(e)
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}
func (s *Server) NodeUnstageVolume(ctx context.Context, q *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	if !validVolumeHandle(q.GetVolumeId()) {
		return nil, status.Error(codes.InvalidArgument, "invalid volume handle")
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if e := s.runtime.Unstage(ctx, q.VolumeId, q.GetStagingTargetPath()); e != nil {
		return nil, nodeError(e)
	}
	return &csi.NodeUnstageVolumeResponse{}, nil
}
func (s *Server) NodeGetVolumeStats(ctx context.Context, q *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	if !validVolumeHandle(q.GetVolumeId()) {
		return nil, status.Error(codes.InvalidArgument, "invalid volume handle")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	usage, e := s.runtime.Stats(ctx, q.VolumeId, q.GetVolumePath())
	if e != nil {
		return nil, nodeError(e)
	}
	return &csi.NodeGetVolumeStatsResponse{Usage: []*csi.VolumeUsage{{Unit: csi.VolumeUsage_BYTES, Total: usage.TotalBytes, Used: usage.UsedBytes, Available: usage.AvailableBytes}, {Unit: csi.VolumeUsage_INODES, Total: usage.TotalInodes, Used: usage.UsedInodes, Available: usage.AvailableInodes}}}, nil
}
func (s *Server) NodeExpandVolume(ctx context.Context, q *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	capacity := q.GetCapacityRange()
	required, limit := capacity.GetRequiredBytes(), capacity.GetLimitBytes()
	if !validVolumeHandle(q.GetVolumeId()) || q.GetVolumePath() == "" || !validExpansionRange(required, limit) {
		return nil, status.Error(codes.InvalidArgument, "valid handle, volume path and expansion range required")
	}
	if q.GetVolumeCapability() != nil {
		if e := nodeCapability(q.VolumeCapability); e != nil {
			return nil, e
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	local, e := s.runtime.Local(q.VolumeId)
	if e != nil {
		return nil, nodeError(e)
	}
	_, ownedTarget := local.Targets[q.VolumePath]
	if local.StagePath != q.VolumePath && !ownedTarget {
		return nil, status.Error(codes.NotFound, "path not owned by this volume")
	}
	permit, e := s.permit(ctx, protocol.PublishContext{Node: local.Node, Volume: local.Volume}, false)
	if e != nil {
		return nil, nodeError(e)
	}
	size, e := s.runtime.Expand(ctx, permit, q.GetVolumePath(), required, limit)
	if e != nil {
		return nil, nodeError(e)
	}
	if size < required {
		return nil, status.Error(codes.FailedPrecondition, "actual expanded size below request")
	}
	if limit > 0 && size > limit {
		return nil, status.Error(codes.FailedPrecondition, "actual expansion size exceeds limit")
	}
	return &csi.NodeExpandVolumeResponse{CapacityBytes: size}, nil
}
func (s *Server) NodeGetInfo(ctx context.Context, _ *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	n, e := s.auth.Binding(ctx)
	if e != nil {
		return nil, nodeError(e)
	}
	if n.UID == "" || n.ServerID == "" || n.ProjectID == "" || n.BindingID == "" {
		return nil, status.Error(codes.Unavailable, "verified node binding pending")
	}
	return &csi.NodeGetInfoResponse{NodeId: "clo-node:" + n.ProjectID + ":" + n.ServerID + ":" + n.UID, MaxVolumesPerNode: int64(s.cfg.MaxVolumesPerNode)}, nil
}
func (s *Server) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	var caps []*csi.NodeServiceCapability
	for _, kind := range []csi.NodeServiceCapability_RPC_Type{csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME, csi.NodeServiceCapability_RPC_GET_VOLUME_STATS, csi.NodeServiceCapability_RPC_EXPAND_VOLUME} {
		caps = append(caps, &csi.NodeServiceCapability{Type: &csi.NodeServiceCapability_Rpc{Rpc: &csi.NodeServiceCapability_RPC{Type: kind}}})
	}
	return &csi.NodeGetCapabilitiesResponse{Capabilities: caps}, nil
}
func (s *Server) GetPluginInfo(context.Context, *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{Name: protocol.DriverName, VendorVersion: s.cfg.Version}, nil
}
func (s *Server) GetPluginCapabilities(context.Context, *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{Capabilities: []*csi.PluginCapability{{Type: &csi.PluginCapability_Service_{Service: &csi.PluginCapability_Service{Type: csi.PluginCapability_Service_CONTROLLER_SERVICE}}}, {Type: &csi.PluginCapability_VolumeExpansion_{VolumeExpansion: &csi.PluginCapability_VolumeExpansion{Type: csi.PluginCapability_VolumeExpansion_ONLINE}}}}}, nil
}
func (s *Server) Probe(ctx context.Context, _ *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	n, e := s.auth.Binding(ctx)
	return &csi.ProbeResponse{Ready: wrapperspb.Bool(e == nil && n.BindingID != "")}, nil
}
