#!/bin/sh

set -x

# launch the server
tfi &
PY_PID=$!


clean_up() {
    # Remove our trapped signals.
    trap - TERM
    echo "Killing python server $PY_PID."
    kill -TERM $PY_PID
    echo "Waiting on $PY_PID to exit."
    wait $PY_PID
    exit 0
}

trap clean_up TERM
wait $PY_PID
