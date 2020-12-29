PROJECT     := github.com/amazingchow/photon-dance-vector-space-searcher
SRC         := $(shell find . -type f -name '*.go' -not -path "./vendor/*")
TARGETS     := vector-space-searcher
ALL_TARGETS := $(TARGETS)

all: build

build: $(ALL_TARGETS)

$(TARGETS): $(SRC)
	go build $(GOMODULEPATH)/$(PROJECT)/cmd/$@

test:
	go test -count=1 -v -p 1 $(shell go list ./...)

pb-fmt:
	@clang-format -i ./pb/*.proto

clean:
	rm -f $(ALL_TARGETS)

.PHONY: all build clean