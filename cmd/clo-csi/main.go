package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
	"github.com/google/uuid"
	"io"
	"os"
	"strings"
)

var version = "dev"

type Scope struct {
	Context        string `json:"context"`
	ProjectID      string `json:"projectID"`
	InstallationID string `json:"installationID"`
	Namespace      string `json:"namespace"`
}

func (s Scope) Validate() error {
	if strings.TrimSpace(s.Context) == "" || s.Context != strings.TrimSpace(s.Context) || strings.ContainsAny(s.Context, "\r\n\x00") || s.Namespace == "" || strings.ContainsAny(s.Namespace, "/\r\n\x00 ") {
		return errors.New("explicit context and namespace required")
	}
	for _, v := range []string{s.ProjectID, s.InstallationID} {
		id, e := uuid.Parse(v)
		if e != nil || id.String() != v {
			return errors.New("explicit canonical project and installation UUIDs required")
		}
	}
	return nil
}

type Config struct {
	Scope
	NodeTokenFile, ControllerURL                                                                                                                                                            string
	NodeName, PodName, PodUID, ProcessNonce, Kubeconfig, Endpoint, HealthListen, ChannelListen, AdminSocket, CLOTokenFile, CAFile, CertFile, KeyFile, IdentityFile, JournalDir, KubeletRoot string
	Action, Operator, Nonce, OperationID, BindingID, VolumeID, FenceID, OutFile, InputFile, PreviousTLS, TLSSecret, DNSNames                                                                string
	DryRun                                                                                                                                                                                  bool
}
type Dependencies struct {
	Admin        AdminClient
	AdminFactory func(Config) AdminClient
	LocalAdmin   AdminClient
	Controller   func(Config) error
	Node         func(Config) error
}

const usage = "clo-csi controller|node|admin ACTION|schema\nAdmin: inspect reconcile record-fence recover-attachment clear-quarantine retire-unpublished retire-incomplete-format retire-quiesced-format freeze resume export restore tls-bootstrap\nRequired scope: --context --project-id --installation-id; --namespace defaults to clo-system.\nCloud credentials: controller mounted file only. Recovery IDs are logical record names from inspect; both format retirement actions require the full CSI handle in --volume.\n"

func parse(mode string, args []string) (Config, error) {
	c := Config{Scope: Scope{Namespace: "clo-system"}}
	f := flag.NewFlagSet(mode, flag.ContinueOnError)
	f.SetOutput(io.Discard)
	f.StringVar(&c.Context, "context", "", "")
	f.StringVar(&c.ProjectID, "project-id", "", "")
	f.StringVar(&c.InstallationID, "installation-id", "", "")
	f.StringVar(&c.Namespace, "namespace", "clo-system", "")
	f.StringVar(&c.NodeName, "node-name", os.Getenv("NODE_NAME"), "")
	f.StringVar(&c.PodName, "pod-name", os.Getenv("POD_NAME"), "")
	f.StringVar(&c.PodUID, "pod-uid", os.Getenv("POD_UID"), "")
	f.StringVar(&c.ProcessNonce, "process-nonce", "", "")
	f.StringVar(&c.Kubeconfig, "kubeconfig", "", "")
	f.StringVar(&c.Endpoint, "endpoint", "unix:///csi/csi.sock", "")
	f.StringVar(&c.HealthListen, "health-listen", ":9809", "")
	f.StringVar(&c.ChannelListen, "channel-listen", ":8443", "")
	f.StringVar(&c.AdminSocket, "admin-socket", "/run/clo-csi/admin.sock", "")
	f.StringVar(&c.CLOTokenFile, "clo-token-file", "/etc/clo/credentials/token", "")
	f.StringVar(&c.NodeTokenFile, "node-token-file", "/var/run/clo-csi/token", "")
	f.StringVar(&c.ControllerURL, "controller-url", "", "")
	f.StringVar(&c.CAFile, "ca-file", "/tls/ca.crt", "")
	f.StringVar(&c.CertFile, "tls-cert-file", "/tls/tls.crt", "")
	f.StringVar(&c.KeyFile, "tls-key-file", "/tls/tls.key", "")
	f.StringVar(&c.IdentityFile, "identity-file", "/etc/clo/machine-identity.json", "")
	f.StringVar(&c.JournalDir, "journal-dir", "/var/lib/clo-csi", "")
	f.StringVar(&c.KubeletRoot, "kubelet-root", "/var/lib/kubelet", "")
	f.StringVar(&c.Operator, "operator", os.Getenv("USER"), "")
	f.StringVar(&c.Nonce, "nonce", "", "")
	f.StringVar(&c.OperationID, "operation", "", "")
	f.StringVar(&c.BindingID, "binding", "", "")
	f.StringVar(&c.VolumeID, "volume", "", "")
	f.StringVar(&c.FenceID, "fence", "", "")
	f.StringVar(&c.OutFile, "out", "", "")
	f.StringVar(&c.InputFile, "file", "", "")
	f.StringVar(&c.PreviousTLS, "previous-tls", "", "")
	f.StringVar(&c.TLSSecret, "tls-secret", "clo-csi-tls", "")
	f.StringVar(&c.DNSNames, "dns-names", "", "")
	f.BoolVar(&c.DryRun, "dry-run", false, "")
	if f.Parse(args) != nil || f.NArg() != 0 {
		return c, errors.New("invalid CLI arguments; use --help")
	}
	if e := c.Scope.Validate(); e != nil {
		return c, e
	}
	return c, nil
}
func run(ctx context.Context, args []string, in io.Reader, out io.Writer, deps Dependencies) error {
	if len(args) == 0 {
		return errors.New("mode required; use --help")
	}
	for _, a := range args {
		if a == "--help" || a == "-h" {
			_, e := io.WriteString(out, usage)
			return e
		}
	}
	if args[0] == "schema" {
		return json.NewEncoder(out).Encode(map[string]any{"driver": protocol.DriverName, "version": version, "modes": []string{"controller", "node", "admin"}, "requiredScope": []string{"context", "project-id", "installation-id"}, "volumeMode": "Filesystem", "filesystem": "ext4", "accessMode": "RWO"})
	}
	switch args[0] {
	case "controller", "node":
		c, e := parse(args[0], args[1:])
		if e != nil {
			return e
		}
		launch := deps.Controller
		if args[0] == "node" {
			launch = deps.Node
		}
		if launch == nil {
			return errors.New("runtime launcher unavailable")
		}
		if e = launch(c); e != nil {
			return errors.New(args[0] + " failed; inspect scoped runtime status")
		}
		return nil
	case "admin":
		if len(args) < 2 {
			return errors.New("admin action required")
		}
		if args[1] == "local" {
			var request AdminRequest
			if json.NewDecoder(io.LimitReader(in, 32<<20)).Decode(&request) != nil {
				return errors.New("invalid local admin request")
			}
			if e := request.Scope.Validate(); e != nil {
				return e
			}
			client := deps.LocalAdmin
			if client == nil {
				client = NewUnixAdminClient("/run/clo-csi/admin.sock")
			}
			response, e := client.Do(ctx, request)
			if e != nil {
				response = AdminResponse{Error: safeAdminMessage(e)}
			}
			return json.NewEncoder(out).Encode(response)
		}
		c, e := parse("admin", args[2:])
		if e != nil {
			return e
		}
		c.Action = args[1]
		if c.Action == "tls-bootstrap" {
			return writeTLSBootstrap(c, out)
		}
		allowed := map[string]bool{"inspect": true, "reconcile": true, "record-fence": true, "recover-attachment": true, "clear-quarantine": true, "retire-unpublished": true, "retire-incomplete-format": true, "retire-quiesced-format": true, "freeze": true, "resume": true, "export": true, "restore": true}
		if !allowed[c.Action] {
			return errors.New("unknown admin action")
		}
		if c.Operator == "" {
			c.Operator = "operator"
		}
		if c.Nonce == "" {
			c.Nonce = uuid.NewString()
		}
		if _, e = uuid.Parse(c.Nonce); e != nil {
			return errors.New("valid recovery nonce required")
		}
		request := AdminRequest{Scope: c.Scope, Action: c.Action, Recovery: protocol.RecoveryRequest{Action: c.Action, OperationID: c.OperationID, BindingID: c.BindingID, VolumeID: c.VolumeID, FenceID: c.FenceID, Operator: c.Operator, Nonce: c.Nonce, DryRun: c.DryRun}}
		if c.Action == "restore" {
			if c.InputFile == "" {
				return errors.New("restore requires --file")
			}
			request.Data, e = os.ReadFile(c.InputFile)
			if e != nil {
				return errors.New("backup input unavailable")
			}
		}
		if c.Action == "export" && c.OutFile == "" {
			return errors.New("export requires private --out file")
		}
		client := deps.Admin
		if client == nil && deps.AdminFactory != nil {
			client = deps.AdminFactory(c)
		}
		if client == nil {
			return errors.New("admin transport unavailable")
		}
		response, e := client.Do(ctx, request)
		if e != nil {
			return e
		}
		if c.DryRun {
			return json.NewEncoder(out).Encode(response)
		}
		if c.Action == "export" {
			// Validate in an isolated memory repository. A transport bug must not
			// replace the operator's last good backup with empty or foreign data.
			check := registry.New(registry.NewMemoryBackend(), registry.Installation{ID: c.InstallationID, ProjectID: c.ProjectID})
			if check.SetFrozen(ctx, true) != nil || check.Restore(ctx, response.Data) != nil {
				return errors.New("invalid registry backup response")
			}
			if e = writePrivate(c.OutFile, response.Data); e != nil {
				return e
			}
			_, e = fmt.Fprintln(out, "Registry backup written:", c.OutFile)
			return e
		}
		return json.NewEncoder(out).Encode(response)
	default:
		return errors.New("unknown mode; use --help")
	}
}
func controllerArgv(args []string, generate func() (string, error)) ([]string, error) {
	if len(args) == 0 || args[0] != "controller" {
		return nil, errors.New("controller argv required")
	}
	value := ""
	count := 0
	out := []string{args[0]}
	for i := 1; i < len(args); i++ {
		arg := args[i]
		if strings.HasPrefix(arg, "--process-nonce=") {
			count++
			value = strings.TrimPrefix(arg, "--process-nonce=")
			continue
		}
		if arg == "--process-nonce" {
			count++
			if i+1 >= len(args) {
				return nil, errors.New("controller nonce missing")
			}
			i++
			value = args[i]
			continue
		}
		out = append(out, arg)
	}
	if count > 1 {
		return nil, errors.New("duplicate controller nonce")
	}
	if count == 0 {
		var e error
		value, e = generate()
		if e != nil {
			return nil, errors.New("controller nonce generation failed")
		}
	}
	if _, e := uuid.Parse(value); e != nil {
		return nil, errors.New("valid controller nonce required")
	}
	return append(out, "--process-nonce="+value), nil
}
