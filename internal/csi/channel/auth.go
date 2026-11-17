package channel

import (
	"context"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	authv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"strings"
	"time"
)

const Audience = "clo-csi-node"

type Authenticator interface {
	Authenticate(context.Context, string) (protocol.Caller, error)
}
type AuthConfig struct{ Namespace, ServiceAccount, DaemonSet string }
type KubernetesAuthenticator struct {
	client kubernetes.Interface
	cfg    AuthConfig
}

func NewAuthenticator(client kubernetes.Interface, cfg AuthConfig) *KubernetesAuthenticator {
	return &KubernetesAuthenticator{client, cfg}
}
func authRejected() error { return protocol.Fail(protocol.Invalid, "node authentication rejected") }
func authUnavailable() error {
	return protocol.Fail(protocol.Unavailable, "Kubernetes identity verification unavailable")
}
func claim(u authv1.UserInfo, key string) string {
	v := u.Extra["authentication.kubernetes.io/"+key]
	if len(v) != 1 {
		return ""
	}
	return v[0]
}
func (a *KubernetesAuthenticator) Authenticate(ctx context.Context, token string) (protocol.Caller, error) {
	var out protocol.Caller
	if token == "" || strings.TrimSpace(token) != token || len(token) > 32768 || a.cfg.Namespace == "" || a.cfg.ServiceAccount == "" || a.cfg.DaemonSet == "" {
		return out, authRejected()
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	review, e := a.client.AuthenticationV1().TokenReviews().Create(ctx, &authv1.TokenReview{Spec: authv1.TokenReviewSpec{Token: token, Audiences: []string{Audience}}}, metav1.CreateOptions{})
	if e != nil {
		return out, authUnavailable()
	}
	s := review.Status
	audience := false
	for _, v := range s.Audiences {
		audience = audience || v == Audience
	}
	if !s.Authenticated || s.Error != "" || !audience || s.User.Username != "system:serviceaccount:"+a.cfg.Namespace+":"+a.cfg.ServiceAccount {
		return out, authRejected()
	}
	podName, podUID, nodeName, nodeUID := claim(s.User, "pod-name"), claim(s.User, "pod-uid"), claim(s.User, "node-name"), claim(s.User, "node-uid")
	if podName == "" || podUID == "" || nodeName == "" || nodeUID == "" {
		return out, authRejected()
	}
	pod, e := a.client.CoreV1().Pods(a.cfg.Namespace).Get(ctx, podName, metav1.GetOptions{})
	if e != nil {
		return out, authUnavailable()
	}
	if string(pod.UID) != podUID || pod.Namespace != a.cfg.Namespace || pod.Spec.ServiceAccountName != a.cfg.ServiceAccount || pod.Spec.NodeName != nodeName || pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
		return out, authRejected()
	}
	sa, e := a.client.CoreV1().ServiceAccounts(a.cfg.Namespace).Get(ctx, a.cfg.ServiceAccount, metav1.GetOptions{})
	if e != nil {
		return out, authUnavailable()
	}
	if string(sa.UID) != s.User.UID || sa.UID == "" || sa.DeletionTimestamp != nil {
		return out, authRejected()
	}
	ds, e := a.client.AppsV1().DaemonSets(a.cfg.Namespace).Get(ctx, a.cfg.DaemonSet, metav1.GetOptions{})
	if e != nil {
		return out, authUnavailable()
	}
	owner := false
	for _, ref := range pod.OwnerReferences {
		if ref.Controller != nil && *ref.Controller && ref.APIVersion == "apps/v1" && ref.Kind == "DaemonSet" && ref.Name == ds.Name && ref.UID == ds.UID {
			owner = true
		}
	}
	if !owner || ds.UID == "" || ds.DeletionTimestamp != nil {
		return out, authRejected()
	}
	node, e := a.client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if e != nil {
		return out, authUnavailable()
	}
	if string(node.UID) != nodeUID || node.DeletionTimestamp != nil {
		return out, authRejected()
	}
	out = protocol.Caller{NodeName: nodeName, NodeUID: nodeUID, PodUID: podUID}
	for _, addr := range node.Status.Addresses {
		if addr.Type == corev1.NodeInternalIP {
			out.InternalIPs = append(out.InternalIPs, addr.Address)
		}
	}
	return out, nil
}
