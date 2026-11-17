{{- define "clo-csi.image" -}}
{{ .Values.image.repository }}@{{ required "image.digest must be an immutable qualified digest" .Values.image.digest }}
{{- end -}}
{{- define "clo-csi.tolerations" -}}
- key: node-role.kubernetes.io/control-plane
  operator: Exists
  effect: NoSchedule
- key: node-role.kubernetes.io/master
  operator: Exists
  effect: NoSchedule
{{- end -}}
