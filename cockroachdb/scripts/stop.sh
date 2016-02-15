#! /usr/bin/env bash

su -c "supervisorctl -c /home/ubuntu/supervisor.conf stop cockroach" ubuntu
exit 0
