TEST?=$$(go list ./... |grep -v 'vendor')
GOFMT_FILES?=$$(find . -name '*.go' |grep -v vendor)

default: build

build:
	go build -o pg_prefaulter main.go
	GOOS=solaris GOARCH=amd64 go build -o pg_prefaulter.illumos main.go
	gzip -9fk pg_prefaulter.illumos

vet:
	@echo "go vet ."
	@go vet $$(go list ./... | grep -v vendor/) ; if [ $$? -eq 1 ]; then \
		echo ""; \
		echo "Vet found suspicious constructs. Please check the reported constructs"; \
		echo "and fix them if necessary before submitting the code for review."; \
		exit 1; \
	fi

fmt:
	gofmt -s -w $(GOFMT_FILES)

vendor-status:
	@dep status

.PHONY: build vet fmt vendor-status
