COMMANDS := $(wildcard cmd/*)
COMMANDS := $(COMMANDS:cmd/%=%)

TARGETS := $(foreach cmd,$(COMMANDS),bin/$(cmd))
SOURCES := $(foreach cmd,$(COMMANDS),cmd/$(cmd)/$(cmd).go)

ROOTDIR := $(PWD)

vpath %.go $(wildcard cmd/*)

all: $(TARGETS)

.PHONY: $(COMMANDS)
$(COMMANDS):
	make bin/$@

.PHONY: $(TARGETS)
$(TARGETS): bin/%: %.go
	mkdir -p $(dir $@)
	cd $(dir $<) && go build -o $(ROOTDIR)/$@

