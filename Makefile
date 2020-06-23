PROJECT     := github.com/amazingchow/engine-vector-space-search-service
SRC         := $(shell find . -type f -name '*.go' -not -path "./vendor/*")
TARGETS     := engine-vector-space-search-service
ALL_TARGETS := $(TARGETS)

all: build

build: $(ALL_TARGETS)

$(TARGETS): $(SRC)
	go build -ldflags '$(LDFLAGS)' $(GOMODULEPATH)/$(PROJECT)/cmd/$@

test:
	go test -count=1 -v -p 1 $(shell go list ./...)

pb-fmt:
	@clang-format -i ./pb/*.proto

lint:
	@golangci-lint run --skip-dirs=api --deadline=5m

clean:
	rm -f $(ALL_TARGETS)

.PHONY: all build clean