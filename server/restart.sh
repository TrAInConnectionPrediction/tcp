#!/bin/bash

echo "restarting the webserver from group:" $(id -gn)
sudo /bin/systemctl restart webserver.service
