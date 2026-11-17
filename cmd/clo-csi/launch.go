package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/RaveTechnology/clo-cloud-go/bootstrap"
	"github.com/RaveTechnology/clo-cloud-go/cloud"
	"github.com/RaveTechnology/clo-csi/internal/csi/channel"
	"github.com/RaveTechnology/clo-csi/internal/csi/controller"
	"github.com/RaveTechnology/clo-csi/internal/csi/coordinator"
	"github.com/RaveTechnology/clo-csi/internal/csi/node"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	args := os.Args[1:]
	help := false
	for _, a := range args {
		help = help || a == "--help" || a == "-h"
	}
	if len(args) > 0 && args[0] == "controller" && !help {
		if _, e := parse("controller", args[1:]); e != nil {
			fmt.Fprintln(os.Stderr, e)
			os.Exit(2)
		}
		next, e := controllerArgv(args, func() (string, error) { return uuid.NewString(), nil })
		if e != nil {
			fmt.Fprintln(os.Stderr, e)
			os.Exit(2)
		}
		if !reflect.DeepEqual(next, args) {
			executable, e := os.Executable()
			if e == nil {
				e = syscall.Exec(executable, append([]string{executable}, next...), os.Environ())
			}
			if e != nil {
				fmt.Fprintln(os.Stderr, "controller nonce exec failed")
				os.Exit(1)
			}
		}
	}
	deps := Dependencies{Controller: func(c Config) error { return launchController(ctx, c) }, Node: func(c Config) error { return launchNode(ctx, c) }, AdminFactory: func(c Config) AdminClient { return &RemoteAdmin{Kubeconfig: c.Kubeconfig} }}
	if e := run(ctx, args, os.Stdin, os.Stdout, deps); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func kubeConfig(c Config) (*rest.Config, error) {
	var config *rest.Config
	var e error
	if c.Context == "in-cluster" {
		if c.Kubeconfig != "" {
			return nil, errors.New("in-cluster scope conflicts with explicit kubeconfig")
		}
		config, e = rest.InClusterConfig()
	} else {
		rules := clientcmd.NewDefaultClientConfigLoadingRules()
		if c.Kubeconfig != "" {
			rules.ExplicitPath = c.Kubeconfig
		}
		config, e = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{CurrentContext: c.Context}).ClientConfig()
	}
	if e != nil {
		return nil, errors.New("explicit Kubernetes context unavailable")
	}
	config.Timeout = 15 * time.Second
	config.QPS = 10
	config.Burst = 20
	return config, nil
}
func verifyMarker(ctx context.Context, client kubernetes.Interface, c Config) error {
	marker, e := client.CoreV1().ConfigMaps(c.Namespace).Get(ctx, "clo-csi-installation", metav1.GetOptions{})
	if e != nil || marker.Immutable == nil || !*marker.Immutable || marker.Data["projectID"] != c.ProjectID || marker.Data["installationID"] != c.InstallationID || marker.Namespace != c.Namespace {
		return errors.New("immutable installation marker missing or differs")
	}
	return nil
}
func socketListener(endpoint string) (net.Listener, error) {
	if !strings.HasPrefix(endpoint, "unix:///") {
		return nil, errors.New("absolute Unix CSI endpoint required")
	}
	path := strings.TrimPrefix(endpoint, "unix://")
	if filepath.Clean(path) != path || !filepath.IsAbs(path) {
		return nil, errors.New("canonical Unix socket path required")
	}
	if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return nil, e
	}
	if info, e := os.Lstat(path); e == nil {
		if info.Mode()&os.ModeSocket == 0 || info.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("socket path is not an owned Unix socket")
		}
		connection, e := net.DialTimeout("unix", path, 200*time.Millisecond)
		if e == nil {
			connection.Close()
			return nil, errors.New("Unix socket already active")
		}
		if e = os.Remove(path); e != nil {
			return nil, e
		}
	} else if !os.IsNotExist(e) {
		return nil, e
	}
	listener, e := net.Listen("unix", path)
	if e != nil {
		return nil, e
	}
	if e = os.Chmod(path, 0660); e != nil {
		listener.Close()
		return nil, e
	}
	return listener, nil
}
func runtimeLog(message string, e error) {
	var safe *protocol.Error
	if errors.As(e, &safe) {
		log.Printf("%s: %s", message, safe.Message)
	} else {
		log.Printf("%s", message)
	}
}
func launchController(parent context.Context, c Config) error {
	if c.NodeName == "" || c.PodName == "" || c.PodUID == "" || c.ProcessNonce == "" {
		return errors.New("controller Pod/Node identity and argv nonce required")
	}
	config, e := kubeConfig(c)
	if e != nil {
		return e
	}
	client, e := kubernetes.NewForConfig(config)
	if e != nil {
		return errors.New("Kubernetes client initialization failed")
	}
	dynamicClient, e := dynamic.NewForConfig(config)
	if e != nil {
		return errors.New("registry client initialization failed")
	}
	if e = verifyMarker(parent, client, c); e != nil {
		return e
	}
	pod, e := client.CoreV1().Pods(c.Namespace).Get(parent, c.PodName, metav1.GetOptions{})
	if e != nil || string(pod.UID) != c.PodUID || pod.Spec.NodeName != c.NodeName || pod.DeletionTimestamp != nil {
		return errors.New("controller actual Pod/Node identity differs")
	}
	host, e := client.CoreV1().Nodes().Get(parent, c.NodeName, metav1.GetOptions{})
	if e != nil || host.UID == "" || host.Labels["clo.csi.rave-technology.io/enabled"] != "true" || host.Labels["clo.csi.rave-technology.io/controller"] != "true" {
		return errors.New("controller requires enabled CLO host/verifier placement")
	}
	api, e := cloud.NewFileStorage(c.CLOTokenFile)
	if e != nil {
		return e
	}
	if _, e = tls.LoadX509KeyPair(c.CertFile, c.KeyFile); e != nil {
		return errors.New("controller serving TLS material unavailable")
	}
	repo := registry.New(registry.NewKubernetes(dynamicClient, c.Namespace), registry.Installation{ID: c.InstallationID, ProjectID: c.ProjectID})
	queue := channel.NewQueue()
	core := coordinator.New(repo, api, queue, coordinator.Config{ProjectID: c.ProjectID, ControllerNodeUID: string(host.UID), MaxVolumesPerNode: 8})
	auth := channel.NewAuthenticator(client, channel.AuthConfig{Namespace: c.Namespace, ServiceAccount: "clo-csi-node", DaemonSet: "clo-csi-node"})
	service := controller.New(core, version)
	rpc := grpc.NewServer(grpc.MaxRecvMsgSize(1 << 20))
	csi.RegisterIdentityServer(rpc, service)
	csi.RegisterControllerServer(rpc, service)
	socket, e := socketListener(c.Endpoint)
	if e != nil {
		return e
	}
	defer socket.Close()
	adminSocket, e := socketListener("unix://" + c.AdminSocket)
	if e != nil {
		return e
	}
	defer adminSocket.Close()
	healthListener, e := net.Listen("tcp", c.HealthListen)
	if e != nil {
		return e
	}
	defer healthListener.Close()
	channelListener, e := net.Listen("tcp", c.ChannelListen)
	if e != nil {
		return e
	}
	defer channelListener.Close()
	health := &http.Server{Handler: controller.Health(core), ReadHeaderTimeout: 5 * time.Second}
	nodeHTTP := &http.Server{Handler: channel.NewServer(core, auth), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS13, GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
		cert, e := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if e != nil {
			return nil, errors.New("serving TLS material unavailable")
		}
		return &cert, nil
	}}}
	admin := &http.Server{Handler: adminHandler(NewAdminService(core, c.Scope)), ReadHeaderTimeout: 5 * time.Second}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	failures := make(chan error, 4)
	go func() { failures <- rpc.Serve(socket) }()
	go func() { failures <- health.Serve(healthListener) }()
	go func() { failures <- nodeHTTP.ServeTLS(channelListener, "", "") }()
	go func() { failures <- admin.Serve(adminSocket) }()
	go func() {
		for {
			if ctx.Err() != nil {
				return
			}
			if !core.Ready(ctx) {
				if e := core.StartIssuer(ctx, c.PodUID, c.ProcessNonce); e != nil {
					runtimeLog("controller issuer pending", e)
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
		}
	}()
	select {
	case <-ctx.Done():
	case e = <-failures:
		cancel()
	}
	shutdown, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	_ = health.Shutdown(shutdown)
	_ = nodeHTTP.Shutdown(shutdown)
	_ = admin.Shutdown(shutdown)
	stopRPC(rpc)
	if e != nil && !errors.Is(e, http.ErrServerClosed) && !errors.Is(e, grpc.ErrServerStopped) {
		return errors.New("controller listener stopped")
	}
	return nil
}
func stopRPC(server *grpc.Server) {
	done := make(chan struct{})
	go func() { server.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		server.Stop()
	}
}

type bindingState struct {
	mu  sync.RWMutex
	ref protocol.NodeRef
}

func (s *bindingState) get() protocol.NodeRef    { s.mu.RLock(); defer s.mu.RUnlock(); return s.ref }
func (s *bindingState) set(ref protocol.NodeRef) { s.mu.Lock(); s.ref = ref; s.mu.Unlock() }

type nodeAuthority struct {
	client            *channel.Client
	state             *bindingState
	nodeName, project string
}

func (a *nodeAuthority) Register(ctx context.Context, hello protocol.NodeHello) (protocol.NodeRef, error) {
	ref, e := a.client.Register(ctx, hello)
	if e != nil {
		return ref, e
	}
	if ref.Name != a.nodeName || ref.ProjectID != a.project || ref.UID == "" || ref.ServerID == "" || ref.BindingID == "" {
		return ref, protocol.Fail(protocol.Blocked, "controller registration returned different node identity")
	}
	a.state.set(ref)
	return ref, nil
}
func (a *nodeAuthority) Binding(ctx context.Context) (protocol.NodeRef, error) {
	local := a.state.get()
	if local.BindingID == "" {
		return local, protocol.Fail(protocol.Unavailable, "verified local node registration pending")
	}
	remote, e := a.client.Binding(ctx)
	if e != nil {
		return remote, e
	}
	if remote != local {
		return remote, protocol.Fail(protocol.Blocked, "node registration generation changed")
	}
	return remote, nil
}
func (a *nodeAuthority) AuthorizeStage(ctx context.Context, q protocol.StageRequest) (protocol.StagePermit, error) {
	if q.Node != a.state.get() {
		return protocol.StagePermit{}, protocol.Fail(protocol.Blocked, "local stage node differs")
	}
	return a.client.AuthorizeStage(ctx, q)
}
func (a *nodeAuthority) Report(ctx context.Context, r protocol.Receipt) error {
	if r.Node != a.state.get() {
		return protocol.Fail(protocol.Blocked, "local receipt node differs")
	}
	return a.client.Report(ctx, r)
}

type nodeReadiness struct{ state *bindingState }

func (n nodeReadiness) Ready(context.Context) bool { return n.state.get().BindingID != "" }
func launchNode(parent context.Context, c Config) error {
	if c.NodeName == "" {
		return errors.New("actual node name required")
	}
	if c.Endpoint != "unix://"+filepath.Join(c.KubeletRoot, "plugins", protocol.DriverName, "csi.sock") {
		return errors.New("node CSI endpoint must match kubelet plugin registration path")
	}
	info, e := os.Stat(c.IdentityFile)
	if e != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return errors.New("root-owned machine identity0600 required")
	}
	raw, e := os.ReadFile(c.IdentityFile)
	if e != nil {
		return errors.New("machine identity unavailable")
	}
	var identity bootstrap.MachineIdentity
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.DisallowUnknownFields()
	if d.Decode(&identity) != nil || identity.Validate() != nil || identity.ProjectID != c.ProjectID {
		return errors.New("machine identity does not match configured project")
	}
	ca, e := os.ReadFile(c.CAFile)
	if e != nil {
		return errors.New("controller CA unavailable")
	}
	url := c.ControllerURL
	if url == "" {
		url = "https://clo-csi-controller." + c.Namespace + ".svc"
	}
	client, e := channel.NewClient(url, ca, c.NodeTokenFile)
	if e != nil {
		return e
	}
	if _, e = os.ReadFile(c.NodeTokenFile); e != nil {
		return errors.New("projected node token unavailable")
	}
	journal, e := node.NewJournal(c.JournalDir)
	if e != nil {
		return e
	}
	state := &bindingState{}
	authority := &nodeAuthority{client: client, state: state, nodeName: c.NodeName, project: c.ProjectID}
	inspector := &node.LinuxInspector{Runner: node.HostRunner{}}
	filesystem := &node.HostFilesystem{Runner: node.HostRunner{}, Inspector: inspector}
	volumes := &node.FilesystemRuntime{Inspector: inspector, Journal: journal, Filesystem: filesystem, Paths: node.Paths{Root: c.KubeletRoot}, Node: state.get}
	agent := &node.Runtime{Inspector: inspector, Journal: journal, Filesystem: filesystem, Verifier: &node.Verifier{ProcRoot: "/proc", Node: state.get}, Node: state.get, InstallationID: c.InstallationID}
	service := node.NewServer(volumes, authority, node.NodeConfig{KubeletRoot: c.KubeletRoot, Version: version, MaxVolumesPerNode: 8})
	rpc := grpc.NewServer(grpc.MaxRecvMsgSize(1 << 20))
	csi.RegisterIdentityServer(rpc, service)
	csi.RegisterNodeServer(rpc, service)
	listener, e := socketListener(c.Endpoint)
	if e != nil {
		return e
	}
	defer listener.Close()
	healthListener, e := net.Listen("tcp", c.HealthListen)
	if e != nil {
		return e
	}
	defer healthListener.Close()
	health := &http.Server{Handler: controller.Health(nodeReadiness{state}), ReadHeaderTimeout: 5 * time.Second}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	failures := make(chan error, 2)
	go func() { failures <- rpc.Serve(listener) }()
	go func() { failures <- health.Serve(healthListener) }()
	go func() {
		pollStarted := false
		for {
			if ctx.Err() != nil {
				return
			}
			snapshot, e := inspector.Snapshot(ctx)
			if e == nil {
				_, e = authority.Register(ctx, protocol.NodeHello{Identity: identity, Snapshot: snapshot})
			}
			if e != nil {
				runtimeLog("node registration pending", e)
			} else if !pollStarted {
				pollStarted = true
				go func() { _ = client.Run(ctx, agent.Execute) }()
			}
			delay := 10 * time.Second
			if e == nil {
				delay = 5 * time.Minute
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
		}
	}()
	select {
	case <-ctx.Done():
	case e = <-failures:
		cancel()
	}
	shutdown, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	_ = health.Shutdown(shutdown)
	stopRPC(rpc)
	if e != nil && !errors.Is(e, http.ErrServerClosed) && !errors.Is(e, grpc.ErrServerStopped) {
		return errors.New("node listener stopped")
	}
	return nil
}
