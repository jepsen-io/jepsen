CONTROL_CONTAINER ?= jepsen-control
CONTROL_SENDAPAY_ROOT ?= /workspace/sendapay-backend
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

.PHONY: docker-run docker-smoke docker-smoke-append-only docker-partition-smoke docker-partition-smoke-append-only docker-restart-smoke docker-restart-smoke-append-only docker-stress docker-stress-append-only docker-partition-stress docker-partition-stress-append-only docker-restart-stress docker-restart-stress-append-only

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
		ACCOUNT_COUNT=24 \
		INITIAL_BALANCE_CENTS=100000 \
		MAX_TRANSFER_CENTS=2500 \
		CONCURRENCY=12 \
		TIME_LIMIT=90 \
		STAGGER_MS=0 \
		NEMESIS=partition-app-db

docker-partition-stress-append-only:
	$(MAKE) docker-partition-stress APPEND_ONLY=1

docker-restart-stress:
	$(MAKE) docker-run \
		ACCOUNT_COUNT=24 \
		INITIAL_BALANCE_CENTS=100000 \
		MAX_TRANSFER_CENTS=2500 \
		CONCURRENCY=12 \
		TIME_LIMIT=90 \
		STAGGER_MS=0 \
		NEMESIS=restart-db

docker-restart-stress-append-only:
	$(MAKE) docker-restart-stress APPEND_ONLY=1
