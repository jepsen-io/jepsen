#!/bin/bash

echo "Setting up root's authorized_keys" >> /var/log/jepsen-setup.log
mkdir /root/.ssh
chmod 700 /root/.ssh
cp /run/secrets/authorized_keys /root/.ssh/
chmod 600 /root/.ssh/authorized_keys
