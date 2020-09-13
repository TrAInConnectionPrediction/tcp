#!/bin/sh

#code from https://stackoverflow.com/questions/3258243/check-if-pull-needed-in-git
#https://stackoverflow.com/a/246128/7246401
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

cd $SCRIPT_DIR

#update git repository
git remote update > /dev/null

UPSTREAM=@{u}
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
