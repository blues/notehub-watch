#! /bin/bash

# Note that when we run the server we use sudo because it is a Linux
# design constraint that non-supervisors cannot listen on ports
# less than 1024. This was discovered when running on GCS, which
# by default runs our code unprivileged.

set -v
export PATH=$PATH:/usr/local/go/bin
git reset --hard
git pull
# go get -u
go get
go build

sudo ./notehub-watch
