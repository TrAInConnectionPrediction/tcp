#!/bin/bash

if [ "$EUID" -ne 0 ]
  then echo "Please run as root"
  exit
fi

echo setting up virtual env...

#https://stackoverflow.com/a/40950971/7246401
ver=$(python -V 2>&1 | sed 's/.* \([0-9]\).\([0-9]\).*/\1\2/')
if [ "$ver" -lt "35" ]; then
    echo "This script requires python 3.5 or greater"
    exit 1
fi

python -m venv venv

. venv/bin/activate

echo done

echo installing project...

pip install -e .

deactivate

echo done

#add code for pulling the train data here

echo setting up webserver service...

#https://stackoverflow.com/a/246128/7246401
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
dest="/etc/systemd/system/webserver.service"

sudo sed "s|SOURCE_PATH|${SCRIPT_DIR}|g" server/webserver.service > ${dest}
sudo sed -i "s|SERVER_IP|${1:-"10.16.1.200:5000"}|g" ${dest}
sudo sed -i "s|USERNAME|$(logname)|g" ${dest}

sudo systemctl daemon-reload

echo done

echo starting webserver

sudo chmod +xs server/restart.sh

server/restart.sh

echo done