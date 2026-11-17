package main

import (
	"bytes"
	"gopkg.in/yaml.v3"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func chart(t *testing.T) []map[string]any {
	t.Helper()
	helm, e := exec.LookPath("helm")
	if e != nil {
		home, _ := os.UserHomeDir()
		helm = filepath.Join(home, ".local/bin/helm")
	}
	args := []string{"template", "clo-csi", "../../deploy/csi", "--kube-version=1.36.5", "--namespace=clo-system", "--include-crds", "--set", "projectID=" + testProject, "--set", "installationID=" + testInstall, "--set", "existingSecret=clo-api-lab", "--set", "image.repository=local/clo-csi", "--set", "image.digest=sha256:" + strings.Repeat("1", 64), "--set", "tls.existingSecret=clo-csi-tls"}
	raw, e := exec.Command(helm, args...).CombinedOutput()
	if e != nil {
		t.Fatalf("helm template failed: %s", raw)
	}
	d := yaml.NewDecoder(bytes.NewReader(raw))
	var all []map[string]any
	for {
		var obj map[string]any
		e = d.Decode(&obj)
		if e == io.EOF {
			break
		}
		if e != nil {
			t.Fatal(e)
		}
		if obj["kind"] != nil {
			all = append(all, obj)
		}
	}
	return all
}
func object(t *testing.T, all []map[string]any, kind, name string) map[string]any {
	t.Helper()
	for _, obj := range all {
		if obj["kind"] == kind && obj["metadata"].(map[string]any)["name"] == name {
			return obj
		}
	}
	t.Fatalf("missing %s/%s", kind, name)
	return nil
}
func TestChartSafetyDefaults(t *testing.T) {
	all := chart(t)
	sc := object(t, all, "StorageClass", "clo-volumes")
	if sc["reclaimPolicy"] != "Retain" || sc["volumeBindingMode"] != "WaitForFirstConsumer" || sc["allowVolumeExpansion"] != true {
		t.Fatal("unsafe SC defaults")
	}
	annotations := sc["metadata"].(map[string]any)["annotations"].(map[string]any)
	if annotations["storageclass.kubernetes.io/is-default-class"] != "false" {
		t.Fatal("SC made default")
	}
	dep := object(t, all, "Deployment", "clo-csi-controller")["spec"].(map[string]any)
	if dep["replicas"] != 1 || dep["strategy"].(map[string]any)["type"] != "Recreate" {
		t.Fatal("controller replica/strategy")
	}
	node := object(t, all, "DaemonSet", "clo-csi-node")["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	if node["hostPID"] != true || node["automountServiceAccountToken"] != false {
		t.Fatal("host verifier/default token scope")
	}
	encoded, _ := yaml.Marshal(node)
	if strings.Contains(string(encoded), "clo-api-lab") || strings.Contains(string(encoded), "tls.key") || !strings.Contains(string(encoded), "audience: clo-csi-node") || !strings.Contains(string(encoded), "expirationSeconds: 600") {
		t.Fatal("node token/TLS material scope")
	}
	for _, kind := range []string{"Deployment", "DaemonSet"} {
		objName := "clo-csi-controller"
		if kind == "DaemonSet" {
			objName = "clo-csi-node"
		}
		pod := object(t, all, kind, objName)["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
		if len(pod["tolerations"].([]any)) == 0 || pod["nodeSelector"].(map[string]any)["clo.csi.rave-technology.io/enabled"] != "true" {
			t.Fatal("placement/taints")
		}
		for _, container := range pod["containers"].([]any) {
			c := container.(map[string]any)
			if !regexp.MustCompile(`@sha256:[a-f0-9]{64}$`).MatchString(c["image"].(string)) {
				t.Fatal("unpinned image", c["image"])
			}
		}
	}
	driver := object(t, all, "CSIDriver", "clo.csi.rave-technology.io")["spec"].(map[string]any)
	if driver["attachRequired"] != true || driver["fsGroupPolicy"] != "File" {
		t.Fatal("CSI ownership policy")
	}
	marker := object(t, all, "ConfigMap", "clo-csi-installation")
	if marker["immutable"] != true {
		t.Fatal("installation marker mutable")
	}
	for _, obj := range all {
		if obj["kind"] != "ClusterRole" && obj["kind"] != "Role" {
			continue
		}
		for _, raw := range obj["rules"].([]any) {
			rule := raw.(map[string]any)
			for _, res := range rule["resources"].([]any) {
				if strings.HasPrefix(res.(string), "clovolume") && !strings.HasSuffix(res.(string), "/status") {
					for _, verb := range rule["verbs"].([]any) {
						if verb == "update" || verb == "delete" || verb == "*" {
							t.Fatal("controller has broad update/delete permission on registry objects")
						}
					}
				}
			}
		}
	}
}
func TestBootstrapServiceRoutesUnreadyPod(t *testing.T) {
	all := chart(t)
	service := object(t, all, "Service", "clo-csi-controller")["spec"].(map[string]any)
	if service["type"] != "ClusterIP" || service["publishNotReadyAddresses"] != true {
		t.Fatal("bootstrap inaccessible before ready")
	}
	pod := object(t, all, "Deployment", "clo-csi-controller")["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	for _, c := range pod["containers"].([]any) {
		container := c.(map[string]any)
		if container["name"] != "driver" {
			continue
		}
		live := container["livenessProbe"].(map[string]any)["httpGet"].(map[string]any)
		ready := container["readinessProbe"].(map[string]any)["httpGet"].(map[string]any)
		if live["path"] != "/livez" || ready["path"] != "/readyz" {
			t.Fatal("bootstrap/liveness restart cycle")
		}
	}
}

func TestResizerCanObserveWorkloadPodsAcrossNamespaces(t *testing.T) {
	// The resizer's in-use-volume informer lists Pods cluster-wide. A Role in
	// clo-system cannot synchronize it for PVCs in application namespaces.
	permissions := map[string]bool{}
	for _, obj := range chart(t) {
		if obj["kind"] != "ClusterRole" {
			continue
		}
		for _, raw := range obj["rules"].([]any) {
			rule := raw.(map[string]any)
			core, pods := false, false
			for _, group := range rule["apiGroups"].([]any) {
				core = core || group == ""
			}
			for _, resource := range rule["resources"].([]any) {
				pods = pods || resource == "pods"
			}
			if core && pods {
				for _, verb := range rule["verbs"].([]any) {
					permissions[verb.(string)] = true
				}
			}
		}
	}
	for _, verb := range []string{"get", "list", "watch"} {
		if !permissions[verb] {
			t.Fatal("resizer cannot observe workload Pods:", verb)
		}
	}
	for _, verb := range []string{"create", "update", "patch", "delete", "*"} {
		if permissions[verb] {
			t.Fatal("workload Pod mutation must remain forbidden:", verb)
		}
	}
}
