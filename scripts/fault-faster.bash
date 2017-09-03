#!/bin/bash --

# Usage: cd /var/tmp; while : ; do 2>&1 bash -x fault-faster.sh | grep 'boss thread stats'; done

set -e
set -u

declare -a childs

PGDATA=/manatee/pg/data/
#WALFILES=`/usr/bin/find /manatee/pg/data/pg_xlog/ -type f ! -name '*.done' | /opt/local/bin/sort | /usr/bin/egrep "\`ps auxwww  | grep [r]ecovering | awk '{print $15}' | /opt/local/bin/sed 's/.$//'\`"`
WALFILES=`/usr/bin/find /manatee/pg/data/pg_xlog/ -type f ! -name '*.done' | /opt/local/bin/sort | /usr/bin/egrep "\`2>&1 pgrep postgres | xargs pargs | grep '[r]ecovering' | grep ^argv | awk '{print $6}' | /opt/local/bin/sed 's/.$//'\`"`

exec 2>&1

for w in $WALFILES; do
        ./pg_prefaulter.illumos run \
                --log-level=DEBUG \
                --wal `basename "${w}"` \
                --pgdata "${PGDATA}" \
                --wal-threads 8 \
                --xlogdump-bin=/opt/postgresql/9.6.3/bin/pg_xlogdump \
                --xlog-mode=pg &
        childs+=( "$!" )
done

for (( i = 0; i < ${#childs[@]}; i++)); do
        echo "waiting for child pid ${childs[$i]}"
        wait ${childs[$i]}
done
