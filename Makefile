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

all: volcano-global-scheduler volcano-global-controller-manager volcano-global-webhook-manager

init:
	mkdir -p ${BIN_DIR}
	mkdir -p ${RELEASE_DIR}

volcano-global-scheduler: init
	CC=${CC} CGO_ENABLED=0 go build -ldflags ${LD_FLAGS} -o ${BIN_DIR}/volcano-global-scheduler ./cmd/scheduler

volcano-global-controller-manager: init
	CC=${CC} CGO_ENABLED=0 go build -ldflags ${LD_FLAGS} -o ${BIN_DIR}/volcano-global-controller-manager ./cmd/controller-manager

volcano-global-webhook-manager: init
	CC=${CC} CGO_ENABLED=0 go build -ldflags ${LD_FLAGS} -o ${BIN_DIR}/volcano-global-webhook-manager ./cmd/webhook-manager

images:
	set -e; \
	for name in scheduler controller-manager webhook-manager; do \
		docker buildx build -t "${IMAGE_PREFIX}/volcano-global-$$name:$(TAG)" . -f ./installer/dockerfile/$$name/Dockerfile --output=type=${BUILDX_OUTPUT_TYPE} --platform ${DOCKER_PLATFORMS} --build-arg APK_MIRROR=${APK_MIRROR}; \
	done

clean:
	rm -rf _output/
	rm -f *.log
