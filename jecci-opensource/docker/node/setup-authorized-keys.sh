#!/bin/bash

echo "Setting up root's authorized_keys" >> /var/log/jecci-setup.log
mkdir /root/.ssh
chmod 700 /root/.ssh
cp /run/secrets/authorized_keys /root/.ssh/
chmod 600 /root/.ssh/authorized_keys

echo "Setting up jecci's authorized_keys" >> /var/log/jecci-setup.log
mkdir -p /home/jecci/.ssh
chmod 700 /home/jecci/.ssh
cp /run/secrets/authorized_keys /home/jecci/.ssh/
chmod 600 /home/jecci/.ssh/authorized_keys