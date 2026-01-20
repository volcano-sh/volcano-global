export KUBECONFIG=/etc/karmada/karmada-apiserver.config

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
helm install volcano-global-apiserver "${PROJECT_ROOT}/helm/charts/volcano-global-apiserver" --namespace volcano-global

export KUBECONFIG=~/.kube/config

helm install volcano-global-host "${PROJECT_ROOT}/helm/charts/volcano-global-host" --namespace volcano-global

