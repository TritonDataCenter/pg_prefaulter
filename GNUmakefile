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

### PostgreSQL-specific targets

PGVERSION?=96
POSTGRES?=$(wildcard /usr/local/bin/postgres /opt/local/lib/postgresql$(PGVERSION)/bin/postgres)
PSQL?=$(wildcard /usr/local/bin/psql /opt/local/lib/postgresql$(PGVERSION)/bin/psql)
INITDB?=$(wildcard /usr/local/bin/initdb /opt/local/lib/postgresql$(PGVERSION)/bin/initdb)
PG_CONTROLDATA?=$(wildcard /usr/local/bin/pg_controldata /opt/local/lib/postgresql$(PGVERSION)/bin/pg_controldata)
PWFILE?=.pwfile

PGDATA?=$(GOPATH)/src/github.com/joyent/pg_prefaulter/.pgdata

PGFOLLOWDATA?=$(GOPATH)/src/github.com/joyent/pg_prefaulter/.pgfollowdata
PGFOLLOWPORT=5433

controldata::
	$(PG_CONTROLDATA) -D "$(PGDATA)"

initdb::
	-cat /dev/urandom | strings | grep -o '[[:alnum:]]' | head -n 32 | tr -d '\n' > "$(PWFILE)"
	$(INITDB) --no-locale -U postgres -A md5 --pwfile="$(PWFILE)" -D "$(PGDATA)"
	mkdir -p $(PGDATA) $(PGFOLLOWDATA) || true
	echo "local   replication     postgres                                md5" >> $(PGDATA)/pg_hba.conf
	echo "host    replication     postgres        127.0.0.1/32            md5" >> $(PGDATA)/pg_hba.conf
	echo "host    replication     postgres        ::1/128                 md5" >> $(PGDATA)/pg_hba.conf

initrepl::
	pg_basebackup -R -h localhost -D $(PGFOLLOWDATA) -P -U postgres --xlog-method=stream
	mkdir -p $(PGFOLLOWDATA)/archive || true

startdb::
	2>&1 \
	$(POSTGRES) \
		-D "$(PGDATA)" \
		-c log_connections=on \
		-c log_disconnections=on \
		-c log_duration=on \
		-c log_statement=all \
		-c wal_level=hot_standby \
		-c archive_mode=on \
		-c max_wal_senders=5 \
		-c wal_keep_segments=50 \
		-c hot_standby=on \
		-c archive_command="cp %p $(PGFOLLOWDATA)/archive/%f" \
	| tee postgresql.log

startrepl::
	2>&1 \
	$(POSTGRES) \
		-D "$(PGFOLLOWDATA)" \
		-p "$(PGFOLLOWPORT)" \
		-c log_connections=on \
		-c log_disconnections=on \
		-c log_duration=on \
		-c log_statement=all \
		-c wal_level=hot_standby \
		-c archive_mode=on \
		-c max_wal_senders=5 \
		-c wal_keep_segments=50 \
		-c hot_standby=on \
		-c archive_command="cp %p $(PGFOLLOWDATA)/archive/%f" \
	| tee postgresql.log

cleandb::
	rm -rf "$(PGDATA)"
	rm -f "$(PWFILE)"

cleanrepl::
	rm -rf "$(PGFOLLOWDATA)"

freshdb:: cleandb initdb startdb

freshrepl:: cleanrepl initrepl startrepl

test::
	2>&1 PGSSLMODE=disable PGHOST=/tmp PGUSER=postgres PGPASSWORD="`cat \"$(PWFILE)\"`" make -C ../ testacc TEST=./postgresql | tee test.log

psql::
	env PGPASSWORD="`cat \"$(PWFILE)\"`" $(PSQL) -E postgres postgres

psqlrepl::
	env PGPASSWORD="`cat \"$(PWFILE)\"`" $(PSQL) -p 5433 -E postgres postgres

.PHONY: build controldata initdb startdb cleandb freshdb test psql vet fmt vendor-status initrepl startrepl cleanrepl freshrepl psqlrepl
