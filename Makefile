VERSION=$(shell git tag -l --sort=-v:refname| sed 's/v//g'| head -n 1)
PROJECT='forseti'
DOCKER_HUB='navitia/'$(PROJECT)
REGION='eu-west-1'
SBX_ECR_REGISTRY: '103372532272.dkr.ecr.eu-west-1.amazonaws.com'
PRD_ECR_REGISTRY: '162230498103.dkr.ecr.eu-west-1.amazonaws.com'

GTFS_PROTO='google_transit/gtfs-realtime/proto/gtfs-realtime.proto'

.PHONY: linter-install
linter-install: ## Install linter
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(shell go env GOPATH)/bin v1.46.2

.PHONY: setup
setup: ## Install all the build and lint dependencies
	go get -u golang.org/x/tools/cmd/cover
	go get -u google.golang.org/grpc
	go get -u google.golang.org/protobuf/cmd/protoc-gen-go
	go get -u google.golang.org/grpc/cmd/protoc-gen-go-grpc

.PHONY: test
test: ## Run all the tests
	echo 'mode: atomic' > coverage.txt && FIXTUREDIR=$(CURDIR)/fixtures go test -covermode=atomic -coverprofile=coverage.txt -race -timeout=30s ./...

.PHONY: fasttest
fasttest: ## Run short tests
	echo 'mode: atomic' > coverage.txt && FIXTUREDIR=$(CURDIR)/fixtures go test -short -covermode=atomic -coverprofile=coverage.txt -race -timeout=30s ./...

.PHONY: cover
cover: test ## Run all the tests and opens the coverage report
	go tool cover -html=coverage.txt

.PHONY: fmt
fmt: ## Run goimports on all go files
	find . -name '*.go' -not -wholename './vendor/*' | while read -r file; do goimports -w "$$file"; done

.PHONY: lint
lint: ## Run all the linters
	golangci-lint run -E gosec -E maligned -E misspell -E lll -E prealloc -E goimports -E unparam -E nakedret

.PHONY: ci
ci: lint test ## Run all the tests and code checks

.PHONY: build
build: ## Build a version
	protoc --go_out=. --go_opt=M$(GTFS_PROTO)=/google_transit/ ./$(GTFS_PROTO)
	CGO_ENABLED=0 go build -ldflags '-extldflags "-static"' -ldflags "-X github.com/hove-io/forseti.ForsetiVersion=$(VERSION)" -tags=jsoniter -v ./cmd/...

.PHONY: clean
clean: ## Remove temporary files
	go clean

.PHONY: install
install: ## install project and it's dependancies, useful for autocompletion feature
	go install -i

.PHONY: version
version: ## display version of forseti
	@echo $(VERSION)

.PHONY: docker
docker: build ## build docker image
	docker build -t $(PROJECT):$(VERSION) .

.PHONY: dockerhub-login
dockerhub-login: ## Login Docker hub, DOCKERHUB_USER, DOCKERHUB_PWD, must be provided
	$(info Login Dockerhub)
	echo ${DOCKERHUB_PWD} | docker login --username ${DOCKERHUB_USER} --password-stdin

.PHONY: push-image-forseti-release
push-image-forseti-release: ## Push iforseti-mage to dockerhub
	$(info Push image-forseti-release to Dockerhub)
	docker tag $(PROJECT):$(VERSION) $(DOCKER_HUB):$(VERSION)
	docker tag $(PROJECT):$(VERSION) $(DOCKER_HUB):release
	docker tag $(PROJECT):$(VERSION) $(DOCKER_HUB):latest

	docker push $(DOCKER_HUB):$(VERSION)
	docker push $(DOCKER_HUB):release
	docker push $(DOCKER_HUB):latest

.PHONY: push-image-forseti-master
push-image-forseti-master: ## Push forseti-image to dockerhub
	$(info Push image-forseti-master to Dockerhub)
	docker tag $(PROJECT):$(VERSION) $(DOCKER_HUB):master
	docker push $(DOCKER_HUB):master

.PHONY: push-image-forseti-master-to-sbx
push-image-forseti-master-to-sbx: ## Push forseti-image to aws:sbx
	$(info Push image-forseti-master to aws:sbx)
	docker pull $(DOCKER_HUB):master
	docker tag $(DOCKER_HUB):master $(SBX_ECR_REGISTRY)/$(PROJECT):master
	aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin  $SBX_ECR_REGISTRY
	docker push $(SBX_ECR_REGISTRY)/$(PROJECT):master

# Absolutely awesome: http://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
