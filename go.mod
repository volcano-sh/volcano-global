module volcano.sh/volcano-global

go 1.22.0

require (
	github.com/karmada-io/karmada v1.10.2
	github.com/spf13/pflag v1.0.5
	gomodules.xyz/jsonpatch/v2 v2.4.0
	k8s.io/api v0.30.2
	k8s.io/apimachinery v0.30.2
	k8s.io/client-go v0.30.2
	k8s.io/component-base v0.30.2
	k8s.io/klog/v2 v2.120.1
	k8s.io/kube-openapi v0.0.0-20240620174524-b456828f718b
	sigs.k8s.io/controller-runtime v0.17.5
	volcano.sh/apis v1.10.0
	volcano.sh/volcano v1.10.0
)

// todo: remove this when resourceBinding suspend merge to master
// PR ref: https://github.com/karmada-io/karmada/commit/3058bbf50b953d02166625e2b797cb16c7f26396
replace github.com/karmada-io/karmada => github.com/Vacant2333/karmada v0.0.0-20240606155424-3058bbf50b95

require (
	github.com/Azure/go-ansiterm v0.0.0-20210617225240-d185dfc1b5a1 // indirect
	github.com/MakeNowJust/heredoc v1.0.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/blang/semver/v4 v4.0.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/distribution/reference v0.5.0 // indirect
	github.com/emicklei/go-restful/v3 v3.11.0 // indirect
	github.com/evanphx/json-patch/v5 v5.9.0 // indirect
	github.com/fsnotify/fsnotify v1.7.0 // indirect
	github.com/go-logr/logr v1.4.1 // indirect
	github.com/go-logr/zapr v1.3.0 // indirect
	github.com/go-openapi/jsonpointer v0.20.2 // indirect
	github.com/go-openapi/jsonreference v0.20.4 // indirect
	github.com/go-openapi/swag v0.22.7 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/cadvisor v0.49.0 // indirect
	github.com/google/gnostic-models v0.6.8 // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/google/gofuzz v1.2.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/gorilla/websocket v1.5.0 // indirect
	github.com/imdario/mergo v0.3.16 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/liggitt/tabwriter v0.0.0-20181228230101-89fcab3d43de // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mitchellh/go-wordwrap v1.0.1 // indirect
	github.com/moby/spdystream v0.2.0 // indirect
	github.com/moby/sys/mountinfo v0.6.2 // indirect
	github.com/moby/term v0.0.0-20221205130635-1aeaba878587 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/mxk/go-flowrate v0.0.0-20140419014527-cca7078d478f // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/selinux v1.11.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/client_golang v1.19.0 // indirect
	github.com/prometheus/client_model v0.6.0 // indirect
	github.com/prometheus/common v0.48.0 // indirect
	github.com/prometheus/procfs v0.12.0 // indirect
	github.com/rogpeppe/go-internal v1.11.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/spf13/cobra v1.8.0 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	github.com/stretchr/testify v1.9.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.26.0 // indirect
	golang.org/x/crypto v0.23.0 // indirect
	golang.org/x/exp v0.0.0-20231226003508-02704c960a9b // indirect
	golang.org/x/net v0.25.0 // indirect
	golang.org/x/oauth2 v0.20.0 // indirect
	golang.org/x/sys v0.20.0 // indirect
	golang.org/x/term v0.20.0 // indirect
	golang.org/x/text v0.15.0 // indirect
	golang.org/x/time v0.5.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240528184218-531527333157 // indirect
	google.golang.org/grpc v1.65.0 // indirect
	google.golang.org/protobuf v1.34.1 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	k8s.io/apiextensions-apiserver v0.29.4 // indirect
	k8s.io/apiserver v0.30.2 // indirect
	k8s.io/cli-runtime v0.29.4 // indirect
	k8s.io/cloud-provider v0.25.0 // indirect
	k8s.io/component-helpers v0.30.2 // indirect
	k8s.io/csi-translation-lib v0.30.2 // indirect
	k8s.io/kube-scheduler v0.0.0 // indirect
	k8s.io/kubectl v0.29.4 // indirect
	k8s.io/kubernetes v1.30.2 // indirect
	k8s.io/mount-utils v0.25.0 // indirect
	k8s.io/utils v0.0.0-20231127182322-b307cd553661 // indirect
	sigs.k8s.io/cluster-api v1.7.1 // indirect
	sigs.k8s.io/json v0.0.0-20221116044647-bc3834ca7abd // indirect
	sigs.k8s.io/mcs-api v0.1.0 // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.4.1 // indirect
	sigs.k8s.io/yaml v1.4.0 // indirect
)

replace (
	github.com/vektra/mockery/v2 => github.com/vektra/mockery/v2 v2.10.0
	k8s.io/cli-runtime => k8s.io/cli-runtime v0.29.0
	k8s.io/cluster-bootstrap => k8s.io/cluster-bootstrap v0.29.0
	k8s.io/cri-api => k8s.io/cri-api v0.29.0
	k8s.io/dynamic-resource-allocation => k8s.io/dynamic-resource-allocation v0.29.0
	k8s.io/endpointslice => k8s.io/endpointslice v0.29.0
	k8s.io/kube-aggregator => k8s.io/kube-aggrega v0.29.0
	k8s.io/kube-controller-manager => k8s.io/kube-controller-manager v0.29.0
	k8s.io/kube-proxy => k8s.io/kube-proxy v0.29.0
	k8s.io/kube-scheduler => k8s.io/kube-scheduler v0.29.0
	k8s.io/kubectl => k8s.io/kubectl v0.29.0
	k8s.io/kubelet => k8s.io/kubelet v0.29.0
	k8s.io/legacy-cloud-providers => k8s.io/legacy-cloud-providers v0.29.0
	k8s.io/pod-security-admission => k8s.io/pod-security-admission v0.29.0
	k8s.io/sample-apiserver => k8s.io/sample-apiserver v0.29.0
)
