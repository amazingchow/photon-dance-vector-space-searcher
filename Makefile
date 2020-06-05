PROJECT     := github.com/amazingchow/engine-vector-space-indexing-service
SRC         := $(shell find . -type f -name '*.go' -not -path "./vendor/*")
TARGETS     := engine-vector-space-indexing-service
ALL_TARGETS := $(TARGETS)

all: build

build: $(ALL_TARGETS)

$(TARGETS): $(SRC)
	go build -ldflags '$(LDFLAGS)' $(GOMODULEPATH)/$(PROJECT)/cmd/$@

test:
	go test -count=1 -v -p 1 $(shell go list ./...)

clean:
	rm -f $(ALL_TARGETS)

.PHONY: all build clean