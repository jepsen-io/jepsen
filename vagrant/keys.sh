#!/bin/sh
# generate key pair

su vagrant -c 'ssh-keygen -t rsa -f  ~vagrant/.ssh/id_rsa -N ""'
chmod 0600  ~vagrant/.ssh/id_rsa
