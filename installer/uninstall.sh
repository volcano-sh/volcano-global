export KUBECONFIG=~/.kube/config

helm uninstall volcano-global-host -n volcano-global

export KUBECONFIG=/etc/karmada/karmada-apiserver.config

helm uninstall volcano-global-apiserver --namespace volcano-global
