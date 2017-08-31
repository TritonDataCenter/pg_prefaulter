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
PWFILE?=.pwfile

PGDATA?=$(GOPATH)/src/github.com/joyent/pg_prefaulter/.pgdata

initdb::
	-cat /dev/urandom | strings | grep -o '[[:alnum:]]' | head -n 32 | tr -d '\n' > "$(PWFILE)"
	$(INITDB) --no-locale -U postgres -A md5 --pwfile="$(PWFILE)" -D "$(PGDATA)"

startdb::
	2>&1 \
	$(POSTGRES) \
		-D "$(PGDATA)" \
		-c log_connections=on \
		-c log_disconnections=on \
		-c log_duration=on \
		-c log_statement=all \
	| tee postgresql.log

cleandb::
	rm -rf "$(PGDATA)"
	rm -f "$(PWFILE)"

freshdb:: cleandb initdb startdb

test::
	2>&1 PGSSLMODE=disable PGHOST=/tmp PGUSER=postgres PGPASSWORD="`cat \"$(PWFILE)\"`" make -C ../ testacc TEST=./postgresql | tee test.log

psql::
	env PGPASSWORD="`cat \"$(PWFILE)\"`" $(PSQL) -E postgres postgres

.PHONY: build initdb startdb cleandb freshdb test psql vet fmt vendor-status
