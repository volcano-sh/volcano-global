module volcano.sh/volcano-global

go 1.22.9

require (
	github.com/karmada-io/karmada v1.13.0-alpha.2.0.20250118102547-5bfdf615c712
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.9.0
	gomodules.xyz/jsonpatch/v2 v2.4.0
	k8s.io/api v0.31.3
	k8s.io/apimachinery v0.31.3
	k8s.io/apiserver v0.31.3
	k8s.io/client-go v0.31.3
	k8s.io/component-base v0.31.3
	k8s.io/klog/v2 v2.130.1
	k8s.io/kube-openapi v0.0.0-20240620174524-b456828f718b
	k8s.io/utils v0.0.0-20240711033017-18e509b52bc8
	volcano.sh/apis v1.11.0
	volcano.sh/volcano v1.11.0
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/blang/semver/v4 v4.0.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/distribution/reference v0.5.0 // indirect
	github.com/emicklei/go-restful/v3 v3.12.1 // indirect
	github.com/evanphx/json-patch v5.7.0+incompatible // indirect
	github.com/fsnotify/fsnotify v1.7.0 // indirect
	github.com/fxamacker/cbor/v2 v2.7.0 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-openapi/jsonpointer v0.20.2 // indirect
	github.com/go-openapi/jsonreference v0.20.4 // indirect
	github.com/go-openapi/swag v0.22.9 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/cadvisor v0.49.0 // indirect
	github.com/google/gnostic-models v0.6.8 // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/google/gofuzz v1.2.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/imdario/mergo v0.3.16 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/client_golang v1.19.1 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.55.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/spf13/cobra v1.8.1 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	golang.org/x/net v0.28.0 // indirect
	golang.org/x/oauth2 v0.21.0 // indirect
	golang.org/x/sys v0.23.0 // indirect
	golang.org/x/term v0.23.0 // indirect
	golang.org/x/text v0.17.0 // indirect
	golang.org/x/time v0.5.0 // indirect
	google.golang.org/genproto v0.0.0-20231106174013-bbf56f31fb17 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	k8s.io/apiextensions-apiserver v0.31.3 // indirect
	k8s.io/cloud-provider v0.25.0 // indirect
	k8s.io/component-helpers v0.31.3 // indirect
	k8s.io/csi-translation-lib v0.31.3 // indirect
	k8s.io/kube-scheduler v0.0.0 // indirect
	k8s.io/kubernetes v1.31.3 // indirect
	sigs.k8s.io/controller-runtime v0.19.1 // indirect
	sigs.k8s.io/json v0.0.0-20221116044647-bc3834ca7abd // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.4.1 // indirect
	sigs.k8s.io/yaml v1.4.0 // indirect
)

replace (
	k8s.io/api => k8s.io/api v0.31.3
	k8s.io/cli-runtime => k8s.io/cli-runtime v0.31.3
	k8s.io/cri-api => k8s.io/cri-api v0.31.3
	k8s.io/cri-client => k8s.io/cri-client v0.31.3
	k8s.io/dynamic-resource-allocation => k8s.io/dynamic-resource-allocation v0.31.3
	k8s.io/endpointslice => k8s.io/endpointslice v0.31.3
	k8s.io/kube-controller-manager => k8s.io/kube-controller-manager v0.31.3
	k8s.io/kube-proxy => k8s.io/kube-proxy v0.31.3
	k8s.io/kube-scheduler => k8s.io/kube-scheduler v0.31.3
	k8s.io/kubectl => k8s.io/kubectl v0.31.3
	k8s.io/kubelet => k8s.io/kubelet v0.31.3
	k8s.io/kubernetes => k8s.io/kubernetes v1.31.3
	k8s.io/legacy-cloud-providers => k8s.io/legacy-cloud-providers v0.31.3
	k8s.io/mount-utils => k8s.io/mount-utils v0.31.3
	k8s.io/pod-security-admission => k8s.io/pod-security-admission v0.31.3
	k8s.io/sample-apiserver => k8s.io/sample-apiserver v0.31.3
)
