{{/*
Expand the name of the chart.
*/}}
{{- define "volcano-global.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "volcano-global.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "volcano-global.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "volcano-global.labels" -}}
helm.sh/chart: {{ include "volcano-global.chart" . }}
{{ include "volcano-global.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- if .Values.custom.common_labels }}
{{- toYaml .Values.custom.common_labels }}
{{- end }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "volcano-global.selectorLabels" -}}
app.kubernetes.io/name: {{ include "volcano-global.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Controller labels
*/}}
{{- define "volcano-global.controllerLabels" -}}
{{ include "volcano-global.labels" . }}
app: volcano-global-controller-manager
{{- if .Values.custom.controller_labels }}
{{- toYaml .Values.custom.controller_labels }}
{{- end }}
{{- end }}

{{/*
Webhook labels
*/}}
{{- define "volcano-global.webhookLabels" -}}
{{ include "volcano-global.labels" . }}
app: volcano-global-webhook-manager
{{- if .Values.custom.webhook_labels }}
{{- toYaml .Values.custom.webhook_labels }}
{{- end }}
{{- end }}
