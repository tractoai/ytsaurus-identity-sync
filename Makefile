DATE := $(shell date +"%Y%m%d%H%M")
COMMIT_SHA := $(shell git rev-parse --short HEAD)

VERSION ?= $(DATE)-$(COMMIT_SHA)
IMAGE_NAME ?= ghcr.io/tractoai/ytsaurus-identity-sync
IMAGE_TAG ?= $(VERSION)
PLATFORMS ?= linux/amd64

.PHONY: lint
lint:
	golangci-lint run

.PHONY: lint-fix
lint-fix:
	golangci-lint run --fix

.PHONY: test
test:
	go test ./...

.PHONY: test-fast
test-fast:
	REUSE_YT_CONTAINER=yes go test ./...

.PHONY: format
format:
	go fmt ./...

.PHONY: docker-build
docker-build:
	docker build . --platform "$(PLATFORMS)" -t "$(IMAGE_NAME):$(IMAGE_TAG)"

.PHONY: docker-push
docker-push: docker-build
	docker push "$(IMAGE_NAME):$(IMAGE_TAG)"
