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
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{/*
Controller Manager labels
*/}}
{{- define "volcano-global.controllerManager.labels" -}}
{{ include "volcano-global.labels" . }}
app.kubernetes.io/component: controller-manager
{{- end }}

{{/*
Controller Manager selector labels
*/}}
{{- define "volcano-global.controllerManager.selectorLabels" -}}
app: volcano-global-controller-manager
{{- end }}

{{/*
Webhook Manager labels
*/}}
{{- define "volcano-global.webhookManager.labels" -}}
{{ include "volcano-global.labels" . }}
app.kubernetes.io/component: webhook-manager
{{- end }}

{{/*
Webhook Manager selector labels
*/}}
{{- define "volcano-global.webhookManager.selectorLabels" -}}
app: volcano-global-webhook-manager
{{- end }}

{{/*
Admission Init labels
*/}}
{{- define "volcano-global.admissionInit.labels" -}}
{{ include "volcano-global.labels" . }}
app.kubernetes.io/component: admission-init
{{- end }}

{{/*
Create the namespace
*/}}
{{- define "volcano-global.namespace" -}}
{{- default "volcano-global" .Values.namespace.name }}
{{- end }}

{{/*
Create the image name for controller manager
*/}}
{{- define "volcano-global.controllerManager.image" -}}
{{- $registry := .Values.image.registry }}
{{- $repository := .Values.controllerManager.imageName }}
{{- $tag := .Values.image.tag | default .Chart.AppVersion }}
{{- printf "%s/%s:%s" $registry $repository $tag }}
{{- end }}

{{/*
Create the image name for webhook manager
*/}}
{{- define "volcano-global.webhookManager.image" -}}
{{- $registry := .Values.image.registry }}
{{- $repository := .Values.webhookManager.imageName }}
{{- $tag := .Values.image.tag | default .Chart.AppVersion }}
{{- printf "%s/%s:%s" $registry $repository $tag }}
{{- end }}

{{/*
Build controller manager arguments
*/}}
{{- define "volcano-global.controllerManager.args" -}}
- --kubeconfig={{ .Values.karmada.kubeconfigMountPath }}/{{ .Values.karmada.kubeconfigSecretKey }}
- --leader-elect={{ .Values.controllerManager.leaderElect }}
- --leader-elect-resource-namespace={{ .Values.controllerManager.leaderElectResourceNamespace }}
- --logtostderr
{{- if .Values.controllerManager.healthz.enabled }}
- --enable-healthz=true
- --healthz-address=$(MY_POD_IP):{{ .Values.controllerManager.healthz.port }}
{{- end }}
- --listen-address=$(MY_POD_IP):{{ .Values.controllerManager.metrics.port }}
- --dispatch-period={{ .Values.controllerManager.dispatchPeriod }}
- --controllers={{ .Values.controllerManager.controllers }}
{{- if .Values.controllerManager.reconcilers }}
- --reconcilers={{ .Values.controllerManager.reconcilers }}
{{- end }}
{{- range $key, $value := .Values.controllerManager.featureGates }}
- --feature-gates={{ $key }}={{ $value }}
{{- end }}
- -v={{ .Values.controllerManager.logLevel }}
- 2>&1
{{- end }}

{{/*
Build webhook manager arguments
*/}}
{{- define "volcano-global.webhookManager.args" -}}
- --kubeconfig={{ .Values.karmada.kubeconfigMountPath }}/{{ .Values.karmada.kubeconfigSecretKey }}
- --enabled-admission={{ .Values.webhookManager.enabledAdmissions }}
- --tls-cert-file=/admission.local.config/certificates/tls.crt
- --tls-private-key-file=/admission.local.config/certificates/tls.key
- --ca-cert-file=/admission.local.config/certificates/ca.crt
- --webhook-service-name={{ .Values.webhookManager.serviceName }}
- --webhook-namespace={{ include "volcano-global.namespace" . }}
- --logtostderr
{{- if .Values.webhookManager.healthz.enabled }}
- --enable-healthz=true
- --healthz-address=$(MY_POD_IP):{{ .Values.webhookManager.healthz.port }}
{{- end }}
- --listen-address=$(MY_POD_IP)
- --port={{ .Values.webhookManager.port }}
- -v={{ .Values.webhookManager.logLevel }}
- 2>&1
{{- end }}
