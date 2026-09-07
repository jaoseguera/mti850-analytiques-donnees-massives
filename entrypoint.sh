#!/bin/bash

# Start SSH service
service ssh start

# Start HDFS and YARN daemons
start-dfs.sh
start-yarn.sh

# Start JupyterLab in the background (Access token: mti850)
jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --IdentityProvider.token='mti850' --ServerApp.allow_origin='*' &

# Keep container running and attach interactive shell
exec /bin/bash