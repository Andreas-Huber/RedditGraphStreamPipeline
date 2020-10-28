#!/bin/bash

curl -s https://api.github.com/repos/Dirnei/node-red-contrib-zigbee2mqtt-devices/releases/latest \
| grep "zipball_url" \
| cut -d : -f 2,3 \
| tr -d \"
| tr -d ","

curl -s https://api.github.com/repos/Dirnei/node-red-contrib-zigbee2mqtt-devices/releases/latest | grep browser_download_url | cut -d '"' -f 4

 wget `curl -s https://api.github.com/repos/Dirnei/node-red-contrib-zigbee2mqtt-devices/releases/latest | grep tarball_url | cut -d '"' -f 4`