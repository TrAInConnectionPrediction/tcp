 #!/bin/sh

sudo cp webserver.service /etc/systemd/system/webserver.service

sudo chmod +xs server/restart.sh
