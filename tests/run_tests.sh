#!/usr/bin/env bash

# ./run_tests.sh <Bucket name>

set -u
set -eE

BUCKET="$1"

function cleanup() {
  echo -e "\nCleaning up test bucket"
  gsutil rm -r "$BUCKET/bigbucket" > /dev/null 2>&1 || true

  echo "Cleaning up bigbucket processes"
  for process in $(pgrep bigbucket); do
    kill "$process"
  done
  echo "Done"
}

trap cleanup ERR

# Get directory of script no matter where it's called from
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

echo -e "\nRunning bigbucket server"
$DIR/../bin/bigbucket --bucket "$BUCKET" > /dev/null 2>&1 &

echo -e "\nRunning row tests"
go test $DIR/row_test.go

echo -e "\nRunning column tests"
go test $DIR/column_test.go

echo -e "\nRunning table tests"
go test $DIR/table_test.go

echo -e "\nRunning bigbucket cleaner"
$DIR/../bin/bigbucket --bucket "$BUCKET" --cleaner --cleaner-interval 3 > /dev/null 2>&1 &

echo -e "\nRunning bigbucket cleaner tests"
go test $DIR/cleaner_test.go

echo -e "\nKilling bigbucket cleaner"
kill "$!"

echo -e "\nRunning bigbucket cleaner as HTTP server"
$DIR/../bin/bigbucket --bucket "$BUCKET" --cleaner-http --port 8081 > /dev/null 2>&1 &

echo -e "\nRunning HTTP cleaner tests"
go test $DIR/cleaner_http_test.go

cleanup


