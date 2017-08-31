#!/bin/sh --

set -e

PGDATA=/manatee/pg/data/
WALFILE=`ps auxwww  | grep [r]ecovering | awk '{print $16}'`

exec ./pg_walfaulter.illumos run \
	--log-level=DEBUG \
        --wal "${WALFILE}" \
        --pgdata "${PGDATA}" \
        --xlogdump-bin=./xlogdump \
        --xlog-mode=xlog
