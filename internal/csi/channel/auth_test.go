package channel

import (
	"context"
	appsv1 "k8s.io/api/apps/v1"
	authv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	kt "k8s.io/client-go/testing"
	"reflect"
	"testing"
)

const nodeUID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
const podUID = "99999999-9999-4999-8999-999999999999"
const dsUID = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
const saUID = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"

func authFixture(t *testing.T) (*KubernetesAuthenticator, *fake.Clientset, *authv1.TokenReviewStatus) {
	t.Helper()
	yes := true
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "agent-a", Namespace: "clo-system", UID: types.UID(podUID), OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "DaemonSet", Name: "clo-csi-node", UID: types.UID(dsUID), Controller: &yes}}}, Spec: corev1.PodSpec{NodeName: "worker-a", ServiceAccountName: "clo-csi-node"}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-a", UID: types.UID(nodeUID)}, Status: corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.174.80.4"}}}}
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "clo-csi-node", Namespace: "clo-system", UID: types.UID(saUID)}}
	ds := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "clo-csi-node", Namespace: "clo-system", UID: types.UID(dsUID)}}
	client := fake.NewClientset(pod, node, sa, ds)
	status := &authv1.TokenReviewStatus{Authenticated: true, Audiences: []string{"clo-csi-node"}, User: authv1.UserInfo{Username: "system:serviceaccount:clo-system:clo-csi-node", UID: saUID, Extra: map[string]authv1.ExtraValue{"authentication.kubernetes.io/pod-name": {"agent-a"}, "authentication.kubernetes.io/pod-uid": {podUID}, "authentication.kubernetes.io/node-name": {"worker-a"}, "authentication.kubernetes.io/node-uid": {nodeUID}}}}
	client.PrependReactor("create", "tokenreviews", func(a kt.Action) (bool, runtime.Object, error) {
		r := a.(kt.CreateAction).GetObject().(*authv1.TokenReview)
		if !reflect.DeepEqual(r.Spec.Audiences, []string{"clo-csi-node"}) {
			t.Error("wrong audience")
		}
		if r.Spec.Token != "projected-test-token" {
			t.Error("wrong token passed to TokenReview")
		}
		return true, &authv1.TokenReview{Status: *status}, nil
	})
	return NewAuthenticator(client, AuthConfig{Namespace: "clo-system", ServiceAccount: "clo-csi-node", DaemonSet: "clo-csi-node"}), client, status
}
func TestBoundTokenRequiresPodNodeAndOwner(t *testing.T) {
	ctx := context.Background()
	a, _, _ := authFixture(t)
	caller, e := a.Authenticate(ctx, "projected-test-token")
	if e != nil || caller.PodUID != podUID || caller.NodeUID != nodeUID || len(caller.InternalIPs) != 1 {
		t.Fatalf("valid bound token denied: %+v %v", caller, e)
	}
	for _, which := range []string{"pod-uid", "serviceaccount", "owner-uid", "owner-kind", "deleting"} {
		t.Run(which, func(t *testing.T) {
			a, client, status := authFixture(t)
			pod, _ := client.CoreV1().Pods("clo-system").Get(ctx, "agent-a", metav1.GetOptions{})
			switch which {
			case "pod-uid":
				status.User.Extra["authentication.kubernetes.io/pod-uid"] = authv1.ExtraValue{"other"}
			case "serviceaccount":
				status.User.UID = "other-sa"
			case "owner-uid":
				pod.OwnerReferences[0].UID = "other-ds"
			case "owner-kind":
				pod.OwnerReferences[0].Kind = "Deployment"
			case "deleting":
				now := metav1.Now()
				pod.DeletionTimestamp = &now
			}
			_, _ = client.CoreV1().Pods("clo-system").Update(ctx, pod, metav1.UpdateOptions{})
			if _, e = a.Authenticate(ctx, "projected-test-token"); e == nil {
				t.Fatal("unbound token accepted")
			}
		})
	}
}
func TestCrossNodeAndExpiredTokenRejected(t *testing.T) {
	for _, which := range []string{"node-name", "node-uid", "expired", "audience", "no-pod-uid", "wrong-user"} {
		t.Run(which, func(t *testing.T) {
			a, _, s := authFixture(t)
			switch which {
			case "node-name":
				s.User.Extra["authentication.kubernetes.io/node-name"] = authv1.ExtraValue{"other"}
			case "node-uid":
				s.User.Extra["authentication.kubernetes.io/node-uid"] = authv1.ExtraValue{"other"}
			case "expired":
				s.Authenticated = false
			case "audience":
				s.Audiences = []string{"other"}
			case "no-pod-uid":
				delete(s.User.Extra, "authentication.kubernetes.io/pod-uid")
			case "wrong-user":
				s.User.Username = "system:serviceaccount:other:clo-csi-node"
			}
			if _, e := a.Authenticate(context.Background(), "projected-test-token"); e == nil {
				t.Fatal("invalid token accepted")
			}
		})
	}
}
