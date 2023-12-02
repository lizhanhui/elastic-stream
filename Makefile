DBG_MAKEFILE ?=
ifeq ($(DBG_MAKEFILE),1)
    $(warning ***** starting Makefile for goal(s) "$(MAKECMDGOALS)")
    $(warning ***** $(shell date))
else
    # If we're not debugging the Makefile, don't echo recipes.
    MAKEFLAGS += -s
endif

# The binaries to build (just the basename).
BINS ?= range-server

# The platforms we support.  In theory this can be used for Windows platforms,
# too, but they require specific base images, which we do not have.
ALL_PLATFORMS ?= linux/amd64 linux/arm64

# The "FROM" part of the Dockerfile.  This should be a manifest-list which
# supports all of the platforms listed in ALL_PLATFORMS.
BASE_IMAGE ?= gcr.io/distroless/static

# Where to push the docker images.
REGISTRY ?= docker.io/elasticstream

# This version-strategy uses git tags to set the version string
VERSION ?= $(shell git describe --tags --always --dirty)

# Version of the package, start with digits
DEB_VERSION ?= $(shell echo $(VERSION) | sed -e 's/^[^0-9]*//')

PROFILE ?= dev

# https://doc.rust-lang.org/cargo/guide/build-cache.html
# > For historical reasons, the dev and test profiles are stored in the debug directory,
# > and the release and bench profiles are stored in the release directory.
# > User-defined profiles are stored in a directory with the same name as the profile.
PROFILE_PATH := $(PROFILE)
ifeq ($(PROFILE),dev)
  PROFILE_PATH := debug
else ifeq ($(PROFILE),test)
  PROFILE_PATH := debug
else ifeq ($(PROFILE),bench)
  PROFILE_PATH := release
endif

###
### These variables should not need tweaking.
###

# We don't need make's built-in rules.
MAKEFLAGS += --no-builtin-rules
# Be pedantic about undefined variables.
MAKEFLAGS += --warn-undefined-variables
.SUFFIXES:

# It's necessary to set this because some environments don't link sh -> bash.
SHELL := /usr/bin/env bash -o errexit -o pipefail -o nounset

OS ?= unknown
ifeq ($(OS),Windows_NT)
  HOST_OS := windows
else
  HOST_OS := $(shell uname -s | tr A-Z a-z)
endif
ifeq ($(shell uname -m),x86_64)
  HOST_ARCH := amd64
else
  HOST_ARCH := $(shell uname -m)
endif

# Used internally.  Users should pass RUST_OS and/or RUST_ARCH.
OS := $(if $(RUST_OS),$(RUST_OS),$(HOST_OS))
ARCH := $(if $(RUST_ARCH),$(RUST_ARCH),$(HOST_ARCH))

ifeq ($(OS)_$(ARCH),linux_amd64)
  TARGET := x86_64-unknown-linux-gnu
else ifeq ($(OS)_$(ARCH),linux_arm64)
  TARGET := aarch64-unknown-linux-gnu
else
  $(error Unsupported OS/ARCH $(OS)/$(ARCH))
endif

BIN_EXTENSION :=
ifeq ($(OS), windows)
  BIN_EXTENSION := .exe
endif

TAG := $(VERSION)__$(OS)_$(ARCH)

# This is used in docker buildx commands
BUILDX_NAME := $(shell basename $$(pwd))

FLATC := $(shell command -v flatc 2> /dev/null)

# If you want to build all binaries, see the 'all-build' rule.
# If you want to build all containers, see the 'all-container' rule.
# If you want to build AND push all containers, see the 'all-push' rule.
all: # @HELP builds binaries for one platform ($OS/$ARCH)
all: build

# For the following OS/ARCH expansions, we transform OS/ARCH into OS_ARCH
# because make pattern rules don't match with embedded '/' characters.

build-%:
	$(MAKE) build                         \
	    --no-print-directory              \
	    RUST_OS=$(firstword $(subst _, ,$*)) \
	    RUST_ARCH=$(lastword $(subst _, ,$*))

container-%:
	$(MAKE) container                     \
	    --no-print-directory              \
	    RUST_OS=$(firstword $(subst _, ,$*)) \
	    RUST_ARCH=$(lastword $(subst _, ,$*))

push-%:
	$(MAKE) push                          \
	    --no-print-directory              \
	    RUST_OS=$(firstword $(subst _, ,$*)) \
	    RUST_ARCH=$(lastword $(subst _, ,$*))

deb-%:
	$(MAKE) deb                           \
	    --no-print-directory              \
	    RUST_OS=$(firstword $(subst _, ,$*)) \
	    RUST_ARCH=$(lastword $(subst _, ,$*))

all-build: # @HELP builds binaries for all platforms
all-build: $(addprefix build-, $(subst /,_, $(ALL_PLATFORMS)))

all-container: # @HELP builds containers for all platforms
all-container: $(addprefix container-, $(subst /,_, $(ALL_PLATFORMS)))

all-push: # @HELP pushes containers for all platforms to the defined registry
all-push: $(addprefix push-, $(subst /,_, $(ALL_PLATFORMS)))

all-deb: # @HELP builds debian packages for all platforms
all-deb: $(addprefix deb-, $(subst /,_, $(ALL_PLATFORMS)))

TARGETS = $(foreach bin,$(BINS),target/$(TARGET)/$(PROFILE_PATH)/$(bin)$(BIN_EXTENSION))

# We print the target names here, rather than in TARGETS so
# they are always at the end of the output.
build: # @HELP builds the binary for the current platform
build: $(TARGETS)
	for bin in $(BINS); do                                                      \
	    echo "build: target/$(TARGET)/$(PROFILE_PATH)/$${bin}$(BIN_EXTENSION)"; \
	done

$(foreach bin,$(BINS),$(eval $(strip                                       \
	target/$(TARGET)/$(PROFILE_PATH)/$(bin)$(BIN_EXTENSION): BIN = $(bin)  \
)))

# Force to build targets even if they are up-to-date.
FORCE: ;

$(TARGETS): .flatc FORCE
	cross build --target $(TARGET) --profile $(PROFILE) --bin $(BIN)

container:
	echo "TODO"

DEB_FILES = $(foreach bin,$(BINS),dist/$(bin)_$(DEB_VERSION)_$(ARCH).deb)

# We print the deb package names here, rather than in DEB_FILES so
# they are always at the end of the output.
deb debs: # @HELP builds deb packages for one platform ($OS/$ARCH)
deb debs: $(DEB_FILES)
	for bin in $(BINS); do                                    \
	    echo "deb: dist/$${bin}_$(DEB_VERSION)_$(ARCH).deb";  \
	done
	echo

# Each deb-file target can reference a $(BIN) variable.
# This is done in 2 steps to enable target-specific variables.
$(foreach bin,$(BINS),$(eval $(strip                      \
	dist/$(bin)_$(DEB_VERSION)_$(ARCH).deb: BIN = $(bin)  \
)))
$(foreach bin,$(BINS),$(eval  \
	dist/$(bin)_$(DEB_VERSION)_$(ARCH).deb: target/$(TARGET)/$(PROFILE_PATH)/$(bin)$(BIN_EXTENSION) $$(shell find ./dist/$(bin) -type f)  \
))

# This is the target definition for all deb-files.
# These are used to track build state in hidden files.
$(DEB_FILES):
	if [ "$(OS)" != "linux" ]; then     \
	    echo "deb: invalid OS: $(OS)";  \
	    exit 1;                         \
	fi
	if [[ ! "$(DEB_VERSION)" =~ ^[0-9]+\.[0-9]+\.[0-9]+ ]]; then   \
	    echo "deb: invalid version: $(DEB_VERSION)";               \
	    exit 1;                                                    \
	fi
	rm -rf .dist/$(BIN)_$(DEB_VERSION)_$(ARCH).tmp
	mkdir -p .dist
	cp -r dist/$(BIN) .dist/$(BIN)_$(DEB_VERSION)_$(ARCH).tmp

	find .dist/$(BIN)_$(DEB_VERSION)_$(ARCH).tmp -type f -name .gitignore -delete
	find .dist/$(BIN)_$(DEB_VERSION)_$(ARCH).tmp -type f -exec  \
	sed -i                                                      \
	    -e 's|{ARG_VERSION}|$(DEB_VERSION)|g'                   \
	    -e 's|{ARG_ARCH}|$(ARCH)|g'                             \
	    {} +
	cp target/$(TARGET)/$(PROFILE_PATH)/$(BIN)$(BIN_EXTENSION) .dist/$(BIN)_$(DEB_VERSION)_$(ARCH).tmp/usr/local/bin/$(BIN)$(BIN_EXTENSION)
	cp etc/*.yaml .dist/$(BIN)_$(DEB_VERSION)_$(ARCH).tmp/etc/$(BIN)/

	dpkg-deb -Zxz --root-owner-group --build .dist/$(BIN)_$(DEB_VERSION)_$(ARCH).tmp dist/$(BIN)_$(DEB_VERSION)_$(ARCH).deb

	rm -rf .dist/$(BIN)_$(DEB_VERSION)_$(ARCH).tmp

sort-deps: # @HELP sorts dependencies in Cargo.toml
sort-deps:
	cargo install cargo-sort
	cargo sort --workspace

.flatc: $(shell find components/protocol/fbs -type f)
	if [ -z "$(FLATC)" ]; then   \
	    echo "flatc not found";  \
	    exit 1;                  \
	fi
	if [ "$$($(FLATC) --version | cut -d' ' -f3)" != "23.5.26" ]; then  \
	    echo "flatc version must be 23.5.26";                           \
	    exit 1;                                                        \
	fi
	cargo build --package protocol
	date > $@

version: # @HELP outputs the version string
version:
	echo $(VERSION)

clean: # @HELP removes built binaries and temporary files
clean: flatc-clean cargo-clean deb-clean

flatc-clean:
	rm -f .flatc

cargo-clean:
	cargo clean

deb-clean:
	rm -rf .dist dist/*.deb

help: # @HELP prints this message
help:
	echo "VARIABLES:"
	echo "  BINS = $(BINS)"
	echo "  OS = $(OS)"
	echo "  ARCH = $(ARCH)"
	echo "  TARGET = $(TARGET)"
	echo "  PROFILE = $(PROFILE)"
	echo "  REGISTRY = $(REGISTRY)"
	echo
	echo "TARGETS:"
	grep -E '^.*: *# *@HELP' $(MAKEFILE_LIST)     \
	    | awk '                                   \
	        BEGIN {FS = ": *# *@HELP"};           \
	        { printf "  %-30s %s\n", $$1, $$2 };  \
	    '
