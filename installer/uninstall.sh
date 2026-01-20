export KUBECONFIG=~/.kube/config

helm uninstall volcano-global-host -namespace volcano-global

export KUBECONFIG=/etc/karmada/karmada-apiserver.config

helm uninstall volcano-global-apiserver --namespace volcano-global
