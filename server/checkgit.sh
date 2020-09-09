#!/bin/sh

UPSTREAM=${1:-'@{u}'}
LOCAL=$(git rev-parse @)
REMOTE=$(git rev-parse "$UPSTREAM")
BASE=$(git merge-base @ "$UPSTREAM")

if [ $LOCAL = $REMOTE ]; then
    printf 1
elif [ $LOCAL = $BASE ]; then
    printf 2
elif [ $REMOTE = $BASE ]; then
    printf 3
else
    printf -1
fi
