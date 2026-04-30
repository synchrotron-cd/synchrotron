{{/*
Helper templates: name truncation, common labels, selector
labels, and ServiceAccount name resolution. Standard Helm
patterns — kept here so resource templates stay small.
*/}}

{{- define "synchrotron.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "synchrotron.fullname" -}}
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

{{- define "synchrotron.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "synchrotron.labels" -}}
helm.sh/chart: {{ include "synchrotron.chart" . }}
{{ include "synchrotron.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: synchrotron
{{- end }}

{{- define "synchrotron.selectorLabels" -}}
app.kubernetes.io/name: {{ include "synchrotron.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "synchrotron.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "synchrotron.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}
