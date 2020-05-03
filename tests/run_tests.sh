#!/usr/bin/env bash

# ./run_tests.sh <Bucket name>

set -u

# Get directory of script no matter where it's called from
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

echo "Running bigbucket server"
$DIR/../bin/bigbucket --bucket "$1" > /dev/null 2>&1 &
echo

echo "Running row tests"
go test $DIR/row_test.go
echo

echo "Running column tests"
go test $DIR/column_test.go
echo

echo "Running table tests"
go test $DIR/table_test.go
echo

echo "Running bigbucket cleaner"
$DIR/../bin/bigbucket --bucket "$1" --cleaner --cleaner-interval 3 > /dev/null 2>&1 &
echo

echo "Running bigbucket cleaner tests"
go test $DIR/cleaner_test.go
echo

echo "Killing bigbucket cleaner"
kill "$!"
echo

echo "Running bigbucket cleaner as HTTP server"
$DIR/../bin/bigbucket --bucket "$1" --cleaner-http --port 8081 > /dev/null 2>&1 &
echo

echo "Running HTTP cleaner tests"
go test $DIR/cleaner_http_test.go
echo

echo "Cleaning up test bucket"
gsutil rm -r "$1/bigbucket"

for process in $(pgrep bigbucket); do
  kill "$process"
done


