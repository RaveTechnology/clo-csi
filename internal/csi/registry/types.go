package registry

import (
	"encoding/json"
	"github.com/RaveTechnology/clo-cloud-go/bootstrap"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"time"
)

const (
	KindVolume    = "CloVolume"
	KindNode      = "CloNodeBinding"
	KindOperation = "CloOperation"
	KindFence     = "CloFence"
	KindIssuer    = "CloControllerEpoch"
)

var Kinds = []string{KindVolume, KindNode, KindOperation, KindFence, KindIssuer}

type Document struct {
	Kind    string          `json:"kind"`
	Name    string          `json:"name"`
	UID     string          `json:"uid,omitempty"`
	Version string          `json:"version,omitempty"`
	Spec    json.RawMessage `json:"spec"`
	Status  json.RawMessage `json:"status"`
}
type Installation struct {
	ID        string `json:"installationID"`
	ProjectID string `json:"projectID"`
}
type Meta struct {
	Name    string `json:"-"`
	UID     string `json:"-"`
	Version string `json:"-"`
}
type VolumeSpec struct {
	InstallationID string `json:"installationID"`
	ProjectID      string `json:"projectID"`
	CSIName        string `json:"csiName"`
	Fingerprint    string `json:"fingerprint"`
	ProviderName   string `json:"providerName"`
	RequiredBytes  int64  `json:"requiredBytes"`
	LimitBytes     int64  `json:"limitBytes"`
	FSType         string `json:"fsType"`
	AccessMode     string `json:"accessMode"`
	PVCUID         string `json:"pvcUID,omitempty"`
}
type VolumeStatus struct {
	RetirementProofSHA256 string                     `json:"retirementProofSHA256,omitempty"`
	Initialized           bool                       `json:"initialized"`
	ProviderID            string                     `json:"providerID"`
	Serial                string                     `json:"serial"`
	SizeGB                int                        `json:"sizeGB"`
	FSUUID                string                     `json:"fsUUID"`
	Initialization        string                     `json:"initialization"`
	NodeBindingID         string                     `json:"nodeBindingID"`
	AttachEpoch           int64                      `json:"attachEpoch"`
	PendingOp             string                     `json:"pendingOp"`
	Barrier               bool                       `json:"barrier"`
	Published             bool                       `json:"published"`
	Deleted               bool                       `json:"deleted"`
	Retired               bool                       `json:"retired"`
	Quarantined           bool                       `json:"quarantined"`
	Reason                string                     `json:"reason"`
	FormatOp              string                     `json:"formatOp"`
	RecoveryRequests      []protocol.RecoveryRequest `json:"recoveryRequests,omitempty"`
}
type VolumeRecord struct {
	Meta
	Spec   VolumeSpec
	Status VolumeStatus
}
type NodeSpec struct {
	InstallationID string                    `json:"installationID"`
	ProjectID      string                    `json:"projectID"`
	Ref            protocol.NodeRef          `json:"ref"`
	Identity       bootstrap.MachineIdentity `json:"identity"`
	PrivateIP      string                    `json:"privateIP"`
}
type NodeStatus struct {
	Initialized       bool                       `json:"initialized"`
	RootSerial        string                     `json:"rootSerial"`
	BootVolumeIDs     []string                   `json:"bootVolumeIDs"`
	PendingEnrollment string                     `json:"pendingEnrollment"`
	Fenced            bool                       `json:"fenced"`
	Quarantined       bool                       `json:"quarantined"`
	Reason            string                     `json:"reason"`
	BootID            string                     `json:"bootID"`
	FenceID           string                     `json:"fenceID,omitempty"`
	RecoveryRequests  []protocol.RecoveryRequest `json:"recoveryRequests,omitempty"`
}
type NodeRecord struct {
	Meta
	Spec   NodeSpec
	Status NodeStatus
}
type Phase string

const (
	Prepared   Phase = "Prepared"
	Dispatched Phase = "Dispatched"
	Completed  Phase = "Completed"
	Uncertain  Phase = "Uncertain"
)

type OperationSpec struct {
	InstallationID string                    `json:"installationID"`
	ProjectID      string                    `json:"projectID"`
	Type           string                    `json:"type"`
	Nonce          string                    `json:"nonce"`
	VolumeRecordID string                    `json:"volumeRecordID"`
	NodeBindingID  string                    `json:"nodeBindingID"`
	Issuer         protocol.IssuerDescriptor `json:"issuer"`
	Before         protocol.Snapshot         `json:"before"`
	APIVolumeIDs   []string                  `json:"apiVolumeIDs"`
	Deadline       time.Time                 `json:"deadline"`
	TargetSizeGB   int                       `json:"targetSizeGB"`
	FenceID        string                    `json:"fenceID,omitempty"`
	LimitBytes     int64                     `json:"limitBytes,omitempty"`
}
type QuiescedRetirementIntent struct {
	Request protocol.RecoveryRequest `json:"request"`
	Job     protocol.Job             `json:"job"`
}
type OperationStatus struct {
	QuiescedRetirement *QuiescedRetirementIntent  `json:"quiescedRetirement,omitempty"`
	Initialized        bool                       `json:"initialized"`
	Phase              Phase                      `json:"phase"`
	Sent               bool                       `json:"sent"`
	Result             string                     `json:"result"`
	Receipt            *protocol.Receipt          `json:"receipt,omitempty"`
	Reason             string                     `json:"reason"`
	RecoveryRequests   []protocol.RecoveryRequest `json:"recoveryRequests,omitempty"`
}
type OperationRecord struct {
	Meta
	Spec   OperationSpec
	Status OperationStatus
}
type FenceSpec struct {
	InstallationID string `json:"installationID"`
	ProjectID      string `json:"projectID"`
	BindingID      string `json:"bindingID"`
	ServerID       string `json:"serverID"`
	NodeUID        string `json:"nodeUID"`
	Epoch          int64  `json:"epoch"`
	Operator       string `json:"operator"`
	Nonce          string `json:"nonce"`
}
type FenceStatus struct {
	Initialized bool                      `json:"initialized"`
	Verified    bool                      `json:"verified"`
	Consumed    bool                      `json:"consumed"`
	Deleted     bool                      `json:"deleted"`
	Issuer      protocol.IssuerDescriptor `json:"issuer"`
}
type FenceRecord struct {
	Meta
	Spec   FenceSpec
	Status FenceStatus
}
type IssuerStatus struct {
	Initialized       bool                       `json:"initialized"`
	Current           protocol.IssuerDescriptor  `json:"current"`
	Prior             []protocol.IssuerProof     `json:"prior"`
	Frozen            bool                       `json:"frozen"`
	RestoreNeedsFence bool                       `json:"restoreNeedsFence"`
	RecoveryRequests  []protocol.RecoveryRequest `json:"recoveryRequests,omitempty"`
}
type IssuerRecord struct {
	Meta
	Spec   Installation
	Status IssuerStatus
}
type ProviderVolume struct {
	ID   string
	Name string
}
