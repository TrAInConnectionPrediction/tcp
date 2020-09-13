 #!/bin/sh

if [ "$EUID" -ne 0 ]
  then echo "Please run as root"
  exit
fi

echo setting up virtual env...

python3 -m venv venv

source venv/bin/activate

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
sudo sed -i "" "s|SERVER_IP|${1:-"10.16.1.200:5000"}|g" ${dest}
sudo sed -i "" "s|USERNAME|$(logname)|g" ${dest}

sudo sytemctl daemon-reload

echo done

echo starting webserver

sudo chmod +xs server/restart.sh

server/restart.sh

echo done