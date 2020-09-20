#!/bin/bash

echo "restarting the webserver from group:" $(/usr/bin/id -gn)
/usr/bin/sudo /bin/systemctl restart webserver.service
