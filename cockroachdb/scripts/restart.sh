#! /usr/bin/env bash

su -c "supervisorctl -c /home/ubuntu/supervisor.conf restart cockroach" ubuntu
exit 0
