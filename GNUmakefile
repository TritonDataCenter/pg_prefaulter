GOFMT_FILES?=$$(find . -name '*.go' |grep -v vendor)
PG_PREFAULTER ?= pg_prefaulter

default:: help

.PHONY: build
build:: $(PG_PREFAULTER) ## 10 Build pg_prefaulter binary

.PHONY: pg_prefaulter
pg_prefaulter::
	go build -ldflags "-X main.commit=`git describe --tags --always` -X main.date=`date +%Y-%m-%d_%H:%d`" -o $@ main.go

.PHONY: check
check:: ## 10 Run go test
	go test -v ./...

.PHONY: vet
vet:: ## 10 vet the binary (excluding dependencies)
	@echo "go vet ."
	@go vet $$(go list ./... | grep -v vendor/) ; if [ $$? -eq 1 ]; then \
		echo ""; \
		echo "Vet found suspicious constructs. Please check the reported constructs"; \
		echo "and fix them if necessary before submitting the code for review."; \
		exit 1; \
	fi

.PHONY: fmt
fmt: ## 10 fmt and simplify the code
	gofmt -s -w $(GOFMT_FILES)

.PHONY: vendor-status
vendor-status: ## 10 Display the vendor/ status
	@dep status

.PHONY: release
release: ## 10 Build a release
	#goreleaser --release-notes=release_notes.md
	@goreleaser

.PHONY: release-snapshot
release-snapshot: ## 10 Build a snapshot release
	@goreleaser --snapshot --skip-validate --rm-dist

### PostgreSQL-specific targets

PGVERSION?=96
POSTGRES?=$(wildcard /usr/local/bin/postgres /opt/local/lib/postgresql$(PGVERSION)/bin/postgres /opt/local/bin/postgres)
PSQL?=$(wildcard /usr/local/bin/psql /opt/local/lib/postgresql$(PGVERSION)/bin/psql /opt/local/bin/psql)
PG_BASEBACKUP?=$(wildcard /usr/local/bin/pg_basebackup /opt/local/lib/postgresql$(PGVERSION)/bin/pg_basebackup /opt/local/bin/pg_basebackup)
INITDB?=$(wildcard /usr/local/bin/initdb /opt/local/lib/postgresql$(PGVERSION)/bin/initdb /opt/local/bin/initdb)
PG_CONTROLDATA?=$(wildcard /usr/local/bin/pg_controldata /opt/local/lib/postgresql$(PGVERSION)/bin/pg_controldata /opt/local/bin/pg_controldata)
PWFILE?=.pwfile

PGBENCH?=$(wildcard /usr/local/bin/pgbench /opt/local/lib/postgresql$(PGVERSION)/bin/pgbench /opt/local/bin/pgbench)
PGBENCH_ARGS?=-j 64 -P 60 -r -T 900 --no-vacuum --protocol=prepared
PGBENCH_INIT_ARGS?=-i -s 10 -F 90

GOPATH?=$(shell go env GOPATH)
PGDATA_PRIMARY?=$(GOPATH)/src/github.com/joyent/pg_prefaulter/.pgdata_primary
PGDATA_FOLLOWER?=$(GOPATH)/src/github.com/joyent/pg_prefaulter/.pgdata_follower

PGFOLLOWPORT=5433

.PHONY: check-pg_controldata
check-pg_controldata::
	@if [ -z "$(PG_CONTROLDATA)" ]; then \
		printf "pg_controldata(1) not found.  Check PostgreSQL installation or set PG_CONTROLDATA=/path/to/pg_controldata"; \
		exit 1; \
	fi

.PHONY: check-initdb
check-initdb::
	@if [ -z "$(INITDB)" ]; then \
		printf "initdb(1) not found.  Check PostgreSQL installation or set INITDB=/path/to/initdb"; \
		exit 1; \
	fi

.PHONY: check-pg_basebackup
check-pg_basebackup::
	@if [ -z "$(PG_BASEBACKUP)" ]; then \
		printf "pg_basebackup(1) not found.  Check PostgreSQL installation or set PG_BASEBACKUP=/path/to/pg_basebackup"; \
		exit 1; \
	fi

.PHONY: check-pgbench
check-pgbench::
	@if [ -z "$(PGBENCH)" ]; then \
		printf "pgbench(1) not found.  Check PostgreSQL installation or set PGBENCH=/path/to/pgbench"; \
		exit 1; \
	fi

.PHONY: check-psql
check-psql::
	@if [ -z "$(PSQL)" ]; then \
		printf "psql(1) not found.  Check PostgreSQL installation or set PSQL=/path/to/psql"; \
		exit 1; \
	fi

.PHONY: check-postgres
check-postgres::
	@if [ -z "$(POSTGRES)" ]; then \
		printf "postgres(1) not found.  Check PostgreSQL installation or set POSTGRES=/path/to/postgres"; \
		exit 1; \
	fi

$(PWFILE):
	-cat /dev/urandom | strings | grep -o '[[:alnum:]]' | head -n 32 | tr -d '\n' > $@

.PHONY: freshdb-primary
freshdb-primary:: cleandb-primary initdb-primary startdb-primary ## 30 Drops and recreates the primary database

.PHONY: initdb-primary
initdb-primary:: $(PWFILE) check-initdb ## 30 initdb(1) a primary database
	$(INITDB) --no-locale -U postgres -A md5 --pwfile="$(PWFILE)" -D "$(PGDATA_PRIMARY)"
	mkdir -p $(PGDATA_PRIMARY) $(PGDATA_FOLLOWER) || true
	echo "local   replication     postgres                                md5" >> $(PGDATA_PRIMARY)/pg_hba.conf
	echo "host    replication     postgres        127.0.0.1/32            md5" >> $(PGDATA_PRIMARY)/pg_hba.conf
	echo "host    replication     postgres        ::1/128                 md5" >> $(PGDATA_PRIMARY)/pg_hba.conf

.PHONY: initdb-follower
initdb-follower:: $(PWFILE) check-pg_basebackup ## 40 initdb(1) a follower database
	env PGPASSWORD="`cat \"$(PWFILE)\"`" $(PG_BASEBACKUP) -R -h localhost -D $(PGDATA_FOLLOWER) -P -U postgres --xlog-method=stream
	mkdir -p $(PGDATA_FOLLOWER)/archive || true

.PHONY: startdb-primary
startdb-primary:: check-postgres ## 30 Start the primary database
	2>&1 \
	exec $(POSTGRES) \
		-D "$(PGDATA_PRIMARY)" \
		-c log_connections=off \
		-c log_disconnections=off \
		-c log_duration=off \
		-c log_statement=ddl \
		-c wal_level=hot_standby \
		-c wal_log_hints=on \
		-c full_page_writes=on \
		-c archive_mode=on \
		-c max_wal_senders=5 \
		-c wal_keep_segments=50 \
		-c hot_standby=on \
		-c archive_command="exit 0" \
		-c synchronous_commit=off \
		-c fsync=off \
	| tee postgresql-primary.log

.PHONY: startdb-primary-debug
startdb-primary-debug:: check-postgres ## 30 Start the primary database with debug-level logging
	2>&1 \
	exec $(POSTGRES) \
		-D "$(PGDATA_PRIMARY)" \
		-c log_connections=on \
		-c log_disconnections=on \
		-c log_duration=on \
		-c log_statement=all \
		-c wal_level=hot_standby \
		-c wal_log_hints=on \
		-c full_page_writes=on \
		-c archive_mode=on \
		-c max_wal_senders=5 \
		-c wal_keep_segments=50 \
		-c hot_standby=on \
		-c archive_command="exit 0" \
		-c synchronous_commit=off \
		-c fsync=off \
	| tee postgresql-primary.log

# Note, the follower config is deliberately slower than the primary
.PHONY: startdb-follower
startdb-follower:: check-postgres ## 40 Start the follower database
	2>&1 \
	exec nice -n 20 \
	$(POSTGRES) \
		-D "$(PGDATA_FOLLOWER)" \
		-p "$(PGFOLLOWPORT)" \
		-c log_connections=on \
		-c log_disconnections=on \
		-c log_duration=off \
		-c log_statement=ddl \
		-c wal_level=hot_standby \
		-c wal_log_hints=on \
		-c full_page_writes=on \
		-c archive_mode=on \
		-c max_wal_senders=5 \
		-c wal_keep_segments=50 \
		-c hot_standby=on \
		-c archive_command="exit 0" \
		-c synchronous_commit=on \
		-c fsync=on \
	| tee postgresql-follower.log

# Note, the follower config is deliberately slower than the primary
.PHONY: startdb-follower-debug
startdb-follower-debug:: check-postgres ## 40 Start the follower database with debug-level logging
	2>&1 \
	exec nice -n 20 \
	$(POSTGRES) \
		-D "$(PGDATA_FOLLOWER)" \
		-p "$(PGFOLLOWPORT)" \
		-c log_connections=on \
		-c log_disconnections=on \
		-c log_duration=on \
		-c log_statement=all \
		-c wal_level=hot_standby \
		-c wal_log_hints=on \
		-c full_page_writes=on \
		-c archive_mode=on \
		-c max_wal_senders=5 \
		-c wal_keep_segments=50 \
		-c hot_standby=on \
		-c archive_command="exit 0" \
		-c synchronous_commit=on \
		-c fsync=on \
	| tee postgresql-follower.log

.PHONY: clean
clean:: cleandb-shard ## 90 Clean target
.PHONY: cleandb-shard
cleandb-shard:: cleandb-primary cleandb-follower ## 90 Clean entire shard
	rm -f "$(PWFILE)"

.PHONY: cleandb-primary
cleandb-primary:: ## 30 Clean primary database
	rm -rf "$(PGDATA_PRIMARY)"

.PHONY: cleandb-follower
cleandb-follower:: ## 40 Clean follower database
	rm -rf "$(PGDATA_FOLLOWER)"

.PHONY: freshdb-follower
freshdb-follower:: cleandb-follower initdb-follower startdb-follower ## 40 Drops and recreates the follower database

.PHONY: testdb
testdb:: check resetdb ## 50 Run database tests

.PHONY: pgbench-init
pgbench-init:: check-pgbench ## 60 Initialize pgbench
	2>&1 env PGSSLMODE=disable PGHOST=/tmp PGUSER=postgres PGPASSWORD="`cat \"$(PWFILE)\"`" "$(PGBENCH)" -i $(PGBENCH_INIT_ARGS)

.PHONY: pgbench
pgbench:: check-pgbench ## 60 Run pgbench(1)
	2>&1 env PGSSLMODE=disable PGHOST=/tmp PGUSER=postgres PGPASSWORD="`cat \"$(PWFILE)\"`" "$(PGBENCH)" $(PGBENCH_ARGS)

.PHONY: resetdb
resetdb:: check-psql dropdb createdb gendata ## 50 Drop and recreate the database
	2>&1 PGSSLMODE=disable PGHOST=/tmp PGUSER=postgres PGPASSWORD="`cat \"$(PWFILE)\"`" "$(PSQL)" postgres -c 'DROP DATABASE test' | tee -a test.log

.PHONY: dropdb
dropdb:: check-psql ## 50 Reset the test database
	2>&1 PGSSLMODE=disable PGHOST=/tmp PGUSER=postgres PGPASSWORD="`cat \"$(PWFILE)\"`" "$(PSQL)" postgres -c 'DROP DATABASE test' | tee -a test.log

.PHONY: createdb
createdb: check-psql ## 50 Create the test database
	2>&1 PGSSLMODE=disable PGHOST=/tmp PGUSER=postgres PGPASSWORD="`cat \"$(PWFILE)\"`" "$(PSQL)" postgres -c 'CREATE DATABASE test' | tee -a test.log
	2>&1 PGSSLMODE=disable PGHOST=/tmp PGUSER=postgres PGPASSWORD="`cat \"$(PWFILE)\"`" "$(PSQL)" test -c 'CREATE TABLE garbage (s INT, md5 TEXT)' | tee -a test.log

.PHONY: controldata
controldata:: check-pg_controldata ## 70 Display pg_controldata(1) of the primary
	$(PG_CONTROLDATA) -D "$(PGDATA_PRIMARY)"

.PHONY: gendata
gendata:: check-psql ## 50 Generate data in the primary
	2>&1 PGSSLMODE=disable PGHOST=/tmp PGUSER=postgres PGPASSWORD="`cat \"$(PWFILE)\"`" PGOPTIONS="-c synchronous_commit=off" "$(PSQL)" test -c 'INSERT INTO garbage SELECT s, md5(random()::text) FROM generate_series(1,1000000) s' | tee -a test.log

.PHONY: psql
psql:: psql-primary ## 70 Open a psql(1) shell to the primary

.PHONY: psql-both
psql-both:: ## 70 Send a psql(1) command to both using -c
	@if [ -z "$(PSQL_ARGS)" ]; then \
		printf "PSQL_ARGS not set."; \
		exit 1; \
	fi
	@$(MAKE) psql-primary PSQL_ARGS="$(PSQL_ARGS)"
	@$(MAKE) psql-follower PSQL_ARGS="$(PSQL_ARGS)"

.PHONY: psql-primary
psql-primary:: check-psql ## 30 Open a psql(1) shell to the primary
	@exec env PGPASSWORD="`cat \"$(PWFILE)\"`" "$(PSQL)" -E postgres postgres $(PSQL_ARGS)

.PHONY: psql-follower
psql-follower:: check-psql ## 40 Open a psql(1) shell to the follower
	@exec env PGPASSWORD="`cat \"$(PWFILE)\"`" "$(PSQL)" -p 5433 -E postgres postgres $(PSQL_ARGS)

.PHONY: help
help:: ## 99 This help message
	@echo "pg_prefaulter make(1) targets:"
	@grep -E '^[a-zA-Z\_\-]+:[:]?.*?## [0-9]+ .*$$' $(MAKEFILE_LIST) | \
		sort -n -t '#' -k3,1 | awk '				\
BEGIN { FS = ":[:]?.*?## "; section = 10; };				\
{									\
	newSect = int($$2);						\
	if (section != newSect) {					\
		section = newSect;					\
		printf "\n";						\
	}								\
	sub("^[0-9]+", "",  $$2);					\
	printf "\033[36m%-15s\033[0m %s\n", $$1, $$2;			\
}'
