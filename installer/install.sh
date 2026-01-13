export KUBECONFIG=/etc/karmada/karmada-apiserver.config

helm install volcano-global-apiserver ./helm/charts/volcano-global-apiserver --namespace volcano-global

export KUBECONFIG=~/.kube/config

helm install volcano-global-host ./helm/charts/volcano-global-host --namespace volcano-global

