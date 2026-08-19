{{- define "portage.fullname" -}}
{{ .Release.Name }}-portage
{{- end -}}

{{- define "portage.labels" -}}
app.kubernetes.io/name: portage
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
DATABASE_URL is remapped explicitly (not via envFrom) since the source
Secret's key name (database.existingSecretKey) may not literally be
"DATABASE_URL", which is the exact env var name control_plane/db.py reads.
*/}}
{{- define "portage.databaseEnv" -}}
- name: DATABASE_URL
  valueFrom:
    secretKeyRef:
      name: {{ required "database.existingSecretName is required" .Values.database.existingSecretName }}
      key: {{ .Values.database.existingSecretKey }}
{{- end -}}

{{/*
Credential env vars are open-ended (whatever PORTAGE_*_ACCESS_KEY names an
ExecutionProfile/StorageProfile's credential_reference expects), so those
pass straight through via envFrom instead.
*/}}
{{- define "portage.credentialsEnvFrom" -}}
{{- if .Values.credentials.existingSecretName }}
- secretRef:
    name: {{ .Values.credentials.existingSecretName }}
{{- end }}
{{- end -}}

{{- define "portage.apiServiceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{ include "portage.fullname" . }}-api
{{- else -}}
default
{{- end -}}
{{- end -}}

{{- define "portage.reconcilerServiceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{ include "portage.fullname" . }}-reconciler
{{- else -}}
default
{{- end -}}
{{- end -}}
