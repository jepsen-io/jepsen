#!/bin/sh
# configure lxc and boxes for running tests
# need to be run as sudo/root

# basic packages
apt-get install git

# install java
# from http://www.webupd8.org/2014/03/how-to-install-oracle-java-8-in-debian.html
echo "deb http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main" | tee /etc/apt/sources.list.d/webupd8team-java.list
echo "deb-src http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main" | tee -a /etc/apt/sources.list.d/webupd8team-java.list
apt-key adv --keyserver keyserver.ubuntu.com --recv-keys EEA14886
apt-get update

echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections
apt-get install oracle-java8-installer

function install_lein() {
    lein=~vagrant/bin/lein
    mkdir -p $(dirname $lein)
    wget -O  $lein https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein
    chmod +x $lein
    echo PATH=$(dirname $lein):$PATH >> ~/.bash_profile
}
