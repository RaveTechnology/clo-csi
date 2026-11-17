package controller

import (
	"context"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func (s *Server) GetPluginInfo(context.Context, *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{Name: protocol.DriverName, VendorVersion: s.version}, nil
}
func (s *Server) GetPluginCapabilities(context.Context, *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{Capabilities: []*csi.PluginCapability{{Type: &csi.PluginCapability_Service_{Service: &csi.PluginCapability_Service{Type: csi.PluginCapability_Service_CONTROLLER_SERVICE}}}, {Type: &csi.PluginCapability_VolumeExpansion_{VolumeExpansion: &csi.PluginCapability_VolumeExpansion{Type: csi.PluginCapability_VolumeExpansion_ONLINE}}}}}, nil
}
func (s *Server) Probe(ctx context.Context, _ *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 2e9)
	defer cancel()
	return &csi.ProbeResponse{Ready: wrapperspb.Bool(s.service.Ready(ctx))}, nil
}
