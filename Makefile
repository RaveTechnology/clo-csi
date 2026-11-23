VERSION ?= 0.1.0-rc.5
.PHONY: build package csi-build csi-package image csi-image test check
build:
	mkdir -p bin
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -buildvcs=false -ldflags='-s -w -buildid= -X main.version=$(VERSION)' -o bin/clo-csi ./cmd/clo-csi
csi-build: build
package: build
	mkdir -p dist
	python3 scripts/check-version.py $(VERSION)
	tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner -cf dist/clo-csi-linux-amd64.tar -C bin clo-csi
	gzip -n -f dist/clo-csi-linux-amd64.tar
	helm package deploy/csi --destination dist
	cp LICENSE NOTICE THIRD_PARTY_NOTICES.txt dist/
	cd dist && sha256sum clo-csi-linux-amd64.tar.gz clo-csi-$(VERSION).tgz LICENSE NOTICE THIRD_PARTY_NOTICES.txt > SHA256SUMS
csi-package: package
image:
	docker buildx build --platform=linux/amd64 --provenance=false --build-arg SOURCE_DATE_EPOCH=0 --build-arg VERSION=$(VERSION) -f Dockerfile.csi -t local/clo-csi:$(VERSION) --load .
csi-image: image
test:
	go test ./...
check:
	go test -race ./...
	go vet ./...
