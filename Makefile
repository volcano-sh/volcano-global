# Copyright 2024 The Volcano Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

BIN_DIR=_output/bin
RELEASE_DIR=_output/release

REPO_PATH=volcano.sh/volcano-global
IMAGE_PREFIX=volcanosh

CRD_OPTIONS ?= "crd:crdVersions=v1,generateEmbeddedObjectMeta=true"
CRD_OPTIONS_EXCLUDE_DESCRIPTION=${CRD_OPTIONS}",maxDescLen=0"

CC ?= "gcc"
BUILDX_OUTPUT_TYPE ?= "docker"

# Get OS architecture
OSARCH=$(shell uname -m)
ifeq ($(OSARCH),x86_64)
GOARCH?=amd64
else ifeq ($(OSARCH),x64)
GOARCH?=amd64
else ifeq ($(OSARCH),aarch64)
GOARCH?=arm64
else ifeq ($(OSARCH),aarch64_be)
GOARCH?=arm64
else ifeq ($(OSARCH),armv8b)
GOARCH?=arm64
else ifeq ($(OSARCH),armv8l)
GOARCH?=arm64
else ifeq ($(OSARCH),i386)
GOARCH?=x86
else ifeq ($(OSARCH),i686)
GOARCH?=x86
else ifeq ($(OSARCH),arm)
GOARCH?=arm
else
GOARCH?=$(OSARCH)
endif

# Run `make images DOCKER_PLATFORMS="linux/amd64,linux/arm64" BUILDX_OUTPUT_TYPE=registry IMAGE_PREFIX=[yourregistry]` to push multi-platform
DOCKER_PLATFORMS ?= "linux/${GOARCH}"

GOOS ?= linux

include Makefile.def

.EXPORT_ALL_VARIABLES:

all: volcano-global-controller-manager volcano-global-webhook-manager

init:
	mkdir -p ${BIN_DIR}
	mkdir -p ${RELEASE_DIR}

volcano-global-controller-manager: init
	CC=${CC} CGO_ENABLED=0 go build -ldflags ${LD_FLAGS} -o ${BIN_DIR}/volcano-global-controller-manager ./cmd/controller-manager

volcano-global-webhook-manager: init
	CC=${CC} CGO_ENABLED=0 go build -ldflags ${LD_FLAGS} -o ${BIN_DIR}/volcano-global-webhook-manager ./cmd/webhook-manager

images:
	set -e; \
	for name in controller-manager webhook-manager; do \
		docker buildx build -t "${IMAGE_PREFIX}/volcano-global-$$name:$(TAG)" . -f ./installer/dockerfile/$$name/Dockerfile --output=type=${BUILDX_OUTPUT_TYPE} --platform ${DOCKER_PLATFORMS} --build-arg APK_MIRROR=${APK_MIRROR}; \
	done

unit-test:
	go test -v -race -coverprofile=coverage.txt -covermode=atomic ./...

clean:
	rm -rf _output/
	rm -f *.log

mod-download-go:
	go mod download

.PHONY: mirror-licenses
mirror-licenses: mod-download-go
	@mkdir -p licenses; \
	GOOS=${OS} go install istio.io/tools/cmd/license-lint@1.25.0; \
	if [ -d licenses ] && [ "$(ls -A licenses 2>/dev/null)" ]; then \
		cd licenses; \
		rm -rf `ls ./ | grep -v LICENSE`; \
		cd -; \
	fi; \
	$(shell go env GOPATH)/bin/license-lint --mirror

.PHONY: lint-licenses
lint-licenses:
	@if test -d licenses; then $(shell go env GOPATH)/bin/license-lint --config config/license-lint.yaml; fi

.PHONY: licenses-check
licenses-check: mirror-licenses
	hack/licenses-check.sh

# find or download controller-gen
# download controller-gen if necessary
controller-gen:
ifeq (, $(shell which controller-gen))
	@{ \
	set -e ;\
	CONTROLLER_GEN_TMP_DIR=$$(mktemp -d) ;\
	cd $$CONTROLLER_GEN_TMP_DIR ;\
	go mod init tmp ;\
	GOOS=${OS} go install sigs.k8s.io/controller-tools/cmd/controller-gen@v0.18.0 ;\
	rm -rf $$CONTROLLER_GEN_TMP_DIR ;\
	}
CONTROLLER_GEN=$(GOBIN)/controller-gen
else
CONTROLLER_GEN=$(shell which controller-gen)
endif

# Generate CRDs
manifests: controller-gen
	go mod vendor
	# generate hyperjob crd with description
	$(CONTROLLER_GEN) $(CRD_OPTIONS) paths="./vendor/volcano.sh/apis/pkg/apis/training/v1alpha1" output:crd:artifacts:config=docs/deploy
	# generate hyperjob crd without description to avoid yaml size limit when using `kubectl apply`
	$(CONTROLLER_GEN) $(CRD_OPTIONS_EXCLUDE_DESCRIPTION) paths="./vendor/volcano.sh/apis/pkg/apis/training/v1alpha1" output:crd:artifacts:config=docs/deploy
	rm -rf vendor
