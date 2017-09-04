#!/bin/sh --

set -e

if [ -z "${MANTA_USER}" ]; then
    echo "MANTA_USER unset"
    exit 1
fi

if [ -z "${MANTA_URL}" ]; then
    echo "MANTA_URL unset"
    exit 1
fi

if [ -z "${MANTA_KEY_ID}" ]; then
    echo "MANTA_KEY_ID unset"
    exit 1
fi

mmkdir -p "/${MANTA_USER}/public/pg_prefaulter/"
set +e
mput -p -f ./dist/pg_prefaulter_checksums.txt "/${MANTA_USER}/public/pg_prefaulter/" || /usr/bin/true
set -e

for f in `find ./dist -name 'pg_prefaulter_*.tar.gz'`
do
    mput -p -f "${f}" "/${MANTA_USER}/public/pg_prefaulter/"
#    gunzip -kf "${f}"
#    f="`basename -s .gz \"${f}\"`"
#    echo muntar -f "./dist/${f}" "/${MANTA_USER}/public/pg_prefaulter/"
done

exit 0
