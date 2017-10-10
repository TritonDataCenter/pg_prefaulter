#!/usr/bin/env bash

set -e
echo "" > coverage.txt

for d in $(go list ./... | grep -v vendor); do
    go test -race -coverprofile=profile.out -covermode=atomic $d
    if [ -f profile.out ]; then
        cat profile.out >> coverage.txt
        rm profile.out
    fi
done

# upload the report
[[ -s coverage.txt ]] || {
    echo "No 'coverage.txt' report found, not sending report"
    exit 1
}
[[ -s .codecov ]] || {
    echo "No codecov.io token found in .codecov, NOT sending report"
    exit 1
}

bash <(curl -s https://codecov.io/bash) -t $(cat .codecov)
