{{/*
=============================================================================
NexusFlow Helm Chart — templates/_helpers.tpl
=============================================================================
*/}}

{{- define "nexusflow.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "nexusflow.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{- define "nexusflow.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Common labels applied to every resource */}}
{{- define "nexusflow.labels" -}}
helm.sh/chart: {{ include "nexusflow.chart" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: {{ .Values.global.partOf }}
{{- end }}

{{/* Namespace shorthand */}}
{{- define "nexusflow.namespace" -}}
{{ .Values.global.namespace }}
{{- end }}

{{/* Name of the shared ConfigMap */}}
{{- define "nexusflow.configMapName" -}}
nexusflow-config
{{- end }}

{{/* Name of the shared Secret */}}
{{- define "nexusflow.secretName" -}}
nexusflow-secrets
{{- end }}

{{/* Init: wait for Postgres TCP */}}
{{- define "nexusflow.initWaitForPostgres" -}}
- name: wait-for-postgres
  image: {{ .Values.initContainers.busyboxImage }}
  command:
    - sh
    - -c
    - |
      until nc -z {{ .Values.config.postgresHost }} {{ .Values.config.postgresPort }}; do
        echo "Waiting for postgres..."; sleep 2;
      done
{{- end }}

{{/* Init: wait for Redis TCP */}}
{{- define "nexusflow.initWaitForRedis" -}}
- name: wait-for-redis
  image: {{ .Values.initContainers.busyboxImage }}
  command:
    - sh
    - -c
    - |
      until nc -z {{ .Values.config.redisHost }} {{ .Values.config.redisPort }}; do
        echo "Waiting for redis..."; sleep 2;
      done
{{- end }}

{{/* Init: wait for db-migrate Job to succeed */}}
{{- define "nexusflow.initWaitForMigration" -}}
- name: wait-for-migration
  image: {{ .Values.initContainers.kubectlImage }}
  command:
    - sh
    - -c
    - |
      echo "Waiting for db-migrate Job to complete..."
      until kubectl get job db-migrate -n {{ include "nexusflow.namespace" . }} \
            -o jsonpath='{.status.succeeded}' 2>/dev/null | grep -q "1"; do
        echo "Migration not yet complete, retrying in 3s..."; sleep 3;
      done
      echo "Migration complete."
{{- end }}

{{/* Shared secret env-vars injected into app containers */}}
{{- define "nexusflow.secretEnvVars" -}}
- name: POSTGRES_USER
  valueFrom:
    secretKeyRef:
      name: {{ include "nexusflow.secretName" . }}
      key: POSTGRES_USER
- name: POSTGRES_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "nexusflow.secretName" . }}
      key: POSTGRES_PASSWORD
- name: DATABASE_URL
  valueFrom:
    secretKeyRef:
      name: {{ include "nexusflow.secretName" . }}
      key: DATABASE_URL
- name: REDIS_URL
  valueFrom:
    secretKeyRef:
      name: {{ include "nexusflow.secretName" . }}
      key: REDIS_URL
{{- end }}

{{/* Shared container securityContext (hardened) */}}
{{- define "nexusflow.containerSecurityContext" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities:
  drop: ["ALL"]
{{- end }}

{{/* Shared pod securityContext for app workloads (uid/gid 1000) */}}
{{- define "nexusflow.appPodSecurityContext" -}}
runAsNonRoot: true
runAsUser: 1000
runAsGroup: 1000
fsGroup: 1000
{{- end }}
