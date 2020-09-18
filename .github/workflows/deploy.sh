#!/bin/bash

curl=$(curl -s --data "key=${1}" ${2})
echo curl response: $curl
code=$(echo $curl | jq .code)
resp=$(echo $curl | jq .resp)
echo "code equals:" $code
if [ $code -ge 0 ] 
  then
  echo Success
  exit 0
else
  echo The deploy failed with message: $resp
  exit -1
fi
