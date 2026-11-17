#!/usr/bin/env bash
# Read-only: no Secret reads, no mutation, no implicit current context.
set -euo pipefail
if [[ $# -ne 3 ]]; then
  printf 'Usage: bash scripts/check-csi-lab.sh CONTEXT EXPECTED_PROJECT_ID EXPECTED_INSTALLATION_ID\n' >&2
  exit 2
fi
csi_context="$1"
csi_project="$2"
csi_installation="$3"
csi_marker="$(kubectl --context="$csi_context" --request-timeout=20s -n clo-system get configmap clo-csi-installation -o 'jsonpath={.data.projectID}{"/"}{.data.installationID}{"/"}{.immutable}')"
if [[ "$csi_marker" != "$csi_project/$csi_installation/true" ]]; then
  printf 'Immutable CSI installation scope differs; refusing inspection\n' >&2
  exit 1
fi
kubectl --context="$csi_context" --request-timeout=20s -n clo-system get deployment/clo-csi-controller daemonset/clo-csi-node -o wide
kubectl --context="$csi_context" --request-timeout=20s -n clo-system get pods -o wide
kubectl --context="$csi_context" --request-timeout=20s get storageclass clo-volumes -o yaml
kubectl --context="$csi_context" --request-timeout=20s get csidriver clo.csi.rave-technology.io -o yaml
kubectl --context="$csi_context" --request-timeout=20s get csinodes -o yaml
kubectl --context="$csi_context" --request-timeout=20s -n clo-system get clovolumes,clonodebindings,clooperations,clofences,clocontrollerepochs
