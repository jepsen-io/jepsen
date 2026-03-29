CONTROL_CONTAINER ?= jepsen-control
CONTROL_SENDAPAY_ROOT ?= /workspace/sendapay-backend
JEPSEN_ROOT ?= $(abspath ..)
SENDAPAY_BACKEND_ROOT ?= /Users/mac/Desktop/repos/sendapay-backend
RUNNER_ROOT ?= $(CURDIR)/docker
NODE_COUNT ?= 5
APP_NODES ?= n1,n3
DB_NODE ?= n2
ACCOUNT_COUNT ?= 3
INITIAL_BALANCE_CENTS ?= 10000
MAX_TRANSFER_CENTS ?= 200
CONCURRENCY ?= 4
TIME_LIMIT ?= 5
STAGGER_MS ?= 0
NEMESIS ?= none
NEMESIS_INTERVAL_S ?= 8
APPEND_ONLY ?= 0
EXTRA_ARGS ?=
SUMMARY_MODE ?= classic
FAULT_STRESS_ACCOUNT_COUNT ?= 24
FAULT_STRESS_INITIAL_BALANCE_CENTS ?= 100000
FAULT_STRESS_MAX_TRANSFER_CENTS ?= 2500
FAULT_STRESS_CONCURRENCY ?= 12
FAULT_STRESS_TIME_LIMIT ?= 90
FAULT_STRESS_STAGGER_MS ?= 0

RUN_ARGS = \
	--topology docker \
	--sendapay-root $(CONTROL_SENDAPAY_ROOT) \
	--app-nodes $(APP_NODES) \
	--db-node $(DB_NODE) \
	--account-count $(ACCOUNT_COUNT) \
	--initial-balance-cents $(INITIAL_BALANCE_CENTS) \
	--max-transfer-cents $(MAX_TRANSFER_CENTS) \
	--concurrency $(CONCURRENCY) \
	--time-limit $(TIME_LIMIT) \
	--stagger-ms $(STAGGER_MS) \
	--nemesis $(NEMESIS) \
	--nemesis-interval-s $(NEMESIS_INTERVAL_S) \
	$(EXTRA_ARGS)

ifeq ($(APPEND_ONLY),1)
APPEND_ONLY_FLAG = --append-only
endif

FAULT_STRESS_ARGS = \
	ACCOUNT_COUNT=$(FAULT_STRESS_ACCOUNT_COUNT) \
	INITIAL_BALANCE_CENTS=$(FAULT_STRESS_INITIAL_BALANCE_CENTS) \
	MAX_TRANSFER_CENTS=$(FAULT_STRESS_MAX_TRANSFER_CENTS) \
	CONCURRENCY=$(FAULT_STRESS_CONCURRENCY) \
	TIME_LIMIT=$(FAULT_STRESS_TIME_LIMIT) \
	STAGGER_MS=$(FAULT_STRESS_STAGGER_MS)

.PHONY: docker-up docker-console docker-web docker-install-jepsen latest-nemesis-summary latest-nemesis-summary-append-only docker-run docker-smoke docker-smoke-append-only docker-partition-smoke docker-partition-smoke-append-only docker-restart-smoke docker-restart-smoke-append-only docker-stress docker-stress-append-only docker-partition-stress docker-partition-stress-append-only docker-restart-stress docker-restart-stress-append-only docker-fault-stress docker-fault-stress-append-only

docker-up:
	JEPSEN_ROOT=$(JEPSEN_ROOT) SENDAPAY_ROOT=$(SENDAPAY_BACKEND_ROOT) $(RUNNER_ROOT)/bin/up --dev --daemon --node-count $(NODE_COUNT)

docker-console:
	CONTROL_CONTAINER=$(CONTROL_CONTAINER) $(RUNNER_ROOT)/bin/console

docker-web:
	CONTROL_CONTAINER=$(CONTROL_CONTAINER) $(RUNNER_ROOT)/bin/web

docker-install-jepsen:
	docker exec $(CONTROL_CONTAINER) bash -lc 'cd /jepsen/jepsen && lein install'

latest-nemesis-summary:
	lein run -m sendapay-bank.summary --latest --mode $(SUMMARY_MODE)

latest-nemesis-summary-append-only:
	$(MAKE) latest-nemesis-summary SUMMARY_MODE=append-only

docker-run:
	docker exec $(CONTROL_CONTAINER) bash -lc 'cd /jepsen/sendapay-bank && lein run -- $(RUN_ARGS) $(APPEND_ONLY_FLAG)'

docker-smoke:
	$(MAKE) docker-run \
		ACCOUNT_COUNT=3 \
		INITIAL_BALANCE_CENTS=10000 \
		MAX_TRANSFER_CENTS=200 \
		CONCURRENCY=4 \
		TIME_LIMIT=5 \
		STAGGER_MS=0

docker-smoke-append-only:
	$(MAKE) docker-smoke APPEND_ONLY=1

docker-partition-smoke:
	$(MAKE) docker-run \
		ACCOUNT_COUNT=3 \
		INITIAL_BALANCE_CENTS=10000 \
		MAX_TRANSFER_CENTS=200 \
		CONCURRENCY=4 \
		TIME_LIMIT=20 \
		STAGGER_MS=0 \
		NEMESIS=partition-app-db

docker-partition-smoke-append-only:
	$(MAKE) docker-partition-smoke APPEND_ONLY=1

docker-restart-smoke:
	$(MAKE) docker-run \
		ACCOUNT_COUNT=3 \
		INITIAL_BALANCE_CENTS=10000 \
		MAX_TRANSFER_CENTS=200 \
		CONCURRENCY=4 \
		TIME_LIMIT=20 \
		STAGGER_MS=0 \
		NEMESIS=restart-db

docker-restart-smoke-append-only:
	$(MAKE) docker-restart-smoke APPEND_ONLY=1

docker-stress:
	$(MAKE) docker-run \
		ACCOUNT_COUNT=24 \
		INITIAL_BALANCE_CENTS=100000 \
		MAX_TRANSFER_CENTS=2500 \
		CONCURRENCY=12 \
		TIME_LIMIT=60 \
		STAGGER_MS=0

docker-stress-append-only:
	$(MAKE) docker-stress APPEND_ONLY=1

docker-partition-stress:
	$(MAKE) docker-run \
		$(FAULT_STRESS_ARGS) \
		NEMESIS=partition-app-db

docker-partition-stress-append-only:
	$(MAKE) docker-partition-stress APPEND_ONLY=1

docker-restart-stress:
	$(MAKE) docker-run \
		$(FAULT_STRESS_ARGS) \
		NEMESIS=restart-db

docker-restart-stress-append-only:
	$(MAKE) docker-restart-stress APPEND_ONLY=1

docker-fault-stress:
	@status=0; \
	$(MAKE) docker-partition-stress APPEND_ONLY=$(APPEND_ONLY); \
	rc=$$?; \
	if [ $$rc -ne 0 ]; then status=$$rc; fi; \
	$(MAKE) docker-restart-stress APPEND_ONLY=$(APPEND_ONLY); \
	rc=$$?; \
	if [ $$rc -ne 0 ] && [ $$status -eq 0 ]; then status=$$rc; fi; \
	exit $$status

docker-fault-stress-append-only:
	$(MAKE) docker-fault-stress APPEND_ONLY=1
