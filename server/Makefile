
#
## Debezium Server
#
# This Makefile prepares a Debezium server distribution with the Planetscale Connector pre-installed, and with a sample
# configuration injected. Run it with `make` or `make dist`.
#

VERBOSE ?= no
DEBUGGER ?= no
TARGET ?= target
DEBEZIUM_VERSION ?= 3.2.1.Final

CONNECTOR_VERSION ?= $(shell cat ./.version)
VERSION ?= $(DEBEZIUM_VERSION)-$(CONNECTOR_VERSION)

DIST ?= $(TARGET)/debezium-server-dist/
RESOURCES ?= $(TARGET)/resources
DOWNLOAD ?= curl -fSL --progress-bar
CONNECTOR_OUT ?= $(CURDIR)/../debezium-planetscale/build
CONNECTOR_JAR_NAME ?= planetscale-debezium-adapter-$(VERSION).jar
CONNECTOR_JAR ?= $(CONNECTOR_OUT)/libs/$(CONNECTOR_JAR_NAME)
DEBEZIUM_SERVER_URL ?= https://repo1.maven.org/maven2/io/debezium/debezium-server-dist/$(DEBEZIUM_VERSION)/debezium-server-dist-$(DEBEZIUM_VERSION).tar.gz
DEBEZIUM_SERVER_ROOT ?= $(RESOURCES)/debezium-server-$(VERSION)
DEBEZIUM_SERVER_TGZ ?= $(DEBEZIUM_SERVER_ROOT).tgz
DEBEZIUM_SERVER_INNER ?= $(DEBEZIUM_SERVER_ROOT)/debezium-server
DEBEZIUM_SERVER_CONFIG ?= $(DEBEZIUM_SERVER_INNER)/config/application.properties
DEBEZIUM_SERVER_LIB ?= $(DEBEZIUM_SERVER_INNER)/lib/$(CONNECTOR_JAR_NAME)
DEBEZIUM_SERVER_INJECTED ?= $(DEBEZIUM_SERVER_CONFIG) $(DEBEZIUM_SERVER_LIB)

DEBUGGER_ARGS ?=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:5005

ifeq ($(VERBOSE),yes)
RULE ?=
else
RULE ?= @
endif

all: dist  ## Build all targets.

info: ## Show info about the release.
	@echo "Debezium Server info:"
	@echo "---------------------------"
	@echo "Debezium Version: $(DEBEZIUM_VERSION)"
	@echo "Connector Version: $(CONNECTOR_VERSION)"
	@echo "Dist Version: $(VERSION)"

run: $(DIST)  ## Build and run the Debezium server.
	@echo "Running Debezium Server \"$(VERSION)\"..."
ifeq ($(DEBUGGER),yes)
	$(RULE)cd $(DEBEZIUM_SERVER_INNER) && JAVA_OPTS="$(DEBUGGER_ARGS)" ./run.sh
else
	$(RULE)cd $(DEBEZIUM_SERVER_INNER) && ./run.sh
endif

dist: $(DIST)  ## Build a Debezium Server distribution.

clean:  ## Clean built targets.
	@echo "Cleaning resources"
	rm -fv $(DEBEZIUM_SERVER_TGZ)
	rm -fr $(TARGET)

$(RESOURCES): $(DEBEZIUM_SERVER_TGZ) $(DEBEZIUM_SERVER_ROOT)
	$(RULE)mkdir -p $(RESOURCES)

$(DEBEZIUM_SERVER_TGZ):
	$(RULE)mkdir -p $(RESOURCES)
	@echo "Downloading Debezium Server ($(DEBEZIUM_VERSION))..."
	$(DOWNLOAD) -o $(DEBEZIUM_SERVER_TGZ) $(DEBEZIUM_SERVER_URL)
	@echo "- at: $(DEBEZIUM_SERVER_TGZ)"

$(DEBEZIUM_SERVER_ROOT):
	@echo "Unpacking Debezium Server dist..."
	$(RULE)mkdir -p $(DEBEZIUM_SERVER_ROOT)
	$(RULE)tar -C $(DEBEZIUM_SERVER_ROOT) -xzvf $(DEBEZIUM_SERVER_TGZ)
	@echo "Server dist ready at: $(DEBEZIUM_SERVER_ROOT)"

$(DEBEZIUM_SERVER_CONFIG):
	@echo "Mounting configuration ('application.properties')..."
	$(RULE)cp -fv ./sample-application.properties $(DEBEZIUM_SERVER_CONFIG)
	$(RULE)-cp -fv ./ps.properties $(DEBEZIUM_SERVER_CONFIG) || echo "No customized Debezium config; skipping."

$(DEBEZIUM_SERVER_LIB):
	@echo "Mounting connector (version: $(CONNECTOR_VERSION))"
	$(RULE)cp -fv $(CONNECTOR_JAR) $(DEBEZIUM_SERVER_LIB)

$(DIST): $(RESOURCES) $(DEBEZIUM_SERVER_INJECTED)
	$(RULE)mkdir -p $(DIST)
	@echo "Debezium Server \"$(VERSION)\" ready, with connector \"$(CONNECTOR_VERSION)\"."
