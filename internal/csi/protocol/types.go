// Package protocol contains the versioned, secret-free controller/node contract.
package protocol

import (
	"context"
	"github.com/RaveTechnology/clo-cloud-go/bootstrap"
	"time"
)

const DriverName = "clo.csi.rave-technology.io"

type Caller struct {
	NodeName    string   `json:"nodeName"`
	NodeUID     string   `json:"nodeUID"`
	PodUID      string   `json:"podUID"`
	InternalIPs []string `json:"internalIPs"`
}
type NodeRef struct {
	Name      string `json:"name"`
	UID       string `json:"uid"`
	BindingID string `json:"bindingID"`
	ProjectID string `json:"projectID"`
	ServerID  string `json:"serverID"`
	Epoch     int64  `json:"epoch"`
}
type VolumeRef struct {
	RecordID    string `json:"recordID"`
	Handle      string `json:"handle"`
	ProviderID  string `json:"providerID"`
	Serial      string `json:"serial"`
	FSUUID      string `json:"fsUUID"`
	AttachEpoch int64  `json:"attachEpoch"`
}
type Disk struct {
	Path        string  `json:"path"`
	Serial      string  `json:"serial"`
	MajorMinor  string  `json:"majorMinor"`
	Root        bool    `json:"root"`
	Partitioned bool    `json:"partitioned"`
	FSType      string  `json:"fsType"`
	FSUUID      string  `json:"fsUUID"`
	SizeBytes   int64   `json:"sizeBytes"`
	Mounts      []Mount `json:"mounts"`
}
type Mount struct {
	Target     string `json:"target"`
	Source     string `json:"source"`
	MajorMinor string `json:"majorMinor"`
	ReadOnly   bool   `json:"readOnly"`
}
type Snapshot struct {
	BootID string `json:"bootID"`
	Disks  []Disk `json:"disks"`
}
type NodeHello struct {
	Identity bootstrap.MachineIdentity `json:"identity"`
	Snapshot Snapshot                  `json:"snapshot"`
}
type IssuerDescriptor struct {
	PodUID       string `json:"podUID"`
	ProcessNonce string `json:"processNonce"`
	NodeUID      string `json:"nodeUID"`
	BindingID    string `json:"bindingID"`
	ServerID     string `json:"serverID"`
	HostBootID   string `json:"hostBootID"`
	HostPID      int    `json:"hostPID"`
	StartTicks   uint64 `json:"startTicks"`
}

func (d IssuerDescriptor) Valid() bool {
	return d.PodUID != "" && d.ProcessNonce != "" && d.NodeUID != "" && d.BindingID != "" && d.ServerID != "" && d.HostBootID != "" && d.HostPID > 0 && d.StartTicks > 0
}

type Verdict string

const (
	Alive   Verdict = "Alive"
	Gone    Verdict = "Gone"
	Unknown Verdict = "Unknown"
)

type IssuerProof struct {
	Descriptor IssuerDescriptor `json:"descriptor"`
	Verdict    Verdict          `json:"verdict"`
	BootID     string           `json:"bootID"`
	Reason     string           `json:"reason"`
}
type Job struct {
	InstallationID string           `json:"installationID"`
	ID             string           `json:"id"`
	Nonce          string           `json:"nonce"`
	Type           string           `json:"type"`
	Node           NodeRef          `json:"node"`
	Volume         VolumeRef        `json:"volume"`
	Issuer         IssuerDescriptor `json:"issuer"`
	PodUID         string           `json:"podUID"`
	ProcessNonce   string           `json:"processNonce"`
}
type Receipt struct {
	BlankChecked      bool               `json:"blankChecked,omitempty"`
	Issuers           []IssuerDescriptor `json:"issuers,omitempty"`
	JobID             string             `json:"jobID"`
	Nonce             string             `json:"nonce"`
	Type              string             `json:"type"`
	Node              NodeRef            `json:"node"`
	Volume            VolumeRef          `json:"volume"`
	Snapshot          Snapshot           `json:"snapshot"`
	Issuer            IssuerDescriptor   `json:"issuer"`
	Proof             IssuerProof        `json:"proof"`
	Quiesced          bool               `json:"quiesced"`
	FilesystemChecked bool               `json:"filesystemChecked"`
	Error             string             `json:"error,omitempty"`
	ObservedAt        time.Time          `json:"observedAt"`
}
type VolumeRequest struct {
	Name          string `json:"name"`
	RequiredBytes int64  `json:"requiredBytes"`
	LimitBytes    int64  `json:"limitBytes"`
	FSType        string `json:"fsType"`
	AccessMode    string `json:"accessMode"`
	PVCUID        string `json:"pvcUID,omitempty"`
}
type PublishContext struct {
	Volume VolumeRef `json:"volume"`
	Node   NodeRef   `json:"node"`
}
type StageRequest struct {
	Volume   VolumeRef `json:"volume"`
	Node     NodeRef   `json:"node"`
	Snapshot Snapshot  `json:"snapshot"`
	Format   bool      `json:"format"`
}
type StagePermit struct {
	Volume         VolumeRef `json:"volume"`
	Node           NodeRef   `json:"node"`
	AllowFormat    bool      `json:"allowFormat"`
	Initialization string    `json:"initialization"`
	OperationID    string    `json:"operationID,omitempty"`
	Nonce          string    `json:"nonce,omitempty"`
	ResumeQuiesced bool      `json:"resumeQuiesced"`
}
type RecoveryRequest struct {
	Action      string `json:"action"`
	OperationID string `json:"operationID"`
	VolumeID    string `json:"volumeID"`
	BindingID   string `json:"bindingID"`
	FenceID     string `json:"fenceID"`
	Operator    string `json:"operator"`
	Nonce       string `json:"nonce"`
	DryRun      bool   `json:"dryRun"`
}
type Channel interface {
	Exchange(context.Context, Job) (Receipt, error)
}
type PollingChannel interface {
	Channel
	Poll(context.Context, NodeRef) (Job, error)
	Complete(context.Context, NodeRef, Receipt) error
}
type Authorizer interface {
	Register(context.Context, NodeHello) (NodeRef, error)
	Binding(context.Context) (NodeRef, error)
	AuthorizeStage(context.Context, StageRequest) (StagePermit, error)
	Report(context.Context, Receipt) error
}
