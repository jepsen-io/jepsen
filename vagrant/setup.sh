#!/bin/sh
# configure lxc and boxes for running tests
# need to be run as sudo/root

# basic packages
apt-get install lxc bridge-utils libvirt-bin debootstrap clusterssh git

# install java
# from http://www.webupd8.org/2014/03/how-to-install-oracle-java-8-in-debian.html
echo "deb http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main" | tee /etc/apt/sources.list.d/webupd8team-java.list
echo "deb-src http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main" | tee -a /etc/apt/sources.list.d/webupd8team-java.list
apt-key adv --keyserver keyserver.ubuntu.com --recv-keys EEA14886
apt-get update

echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections
apt-get install oracle-java8-installer

echo "cgroup  /sys/fs/cgroup  cgroup  defaults  0   0" >> /etc/fstab
mount /sys/fs/cgroup

lxc-checkconfig

function buildbox(id, num) {
    # create box
    SUITE=jessie MIRROR=http://ftp.fr.debian.org/debian lxc-create -n "$id" -t debian
    # update configuration, expecially mac address
    cat > "/var/lib/lxc/$id" <<EOF
lxc.rootfs = /var/lib/lxc/$id/rootfs

# Common configuration
lxc.include = /usr/share/lxc/config/debian.common.conf

# Container specific configuration
lxc.mount = /var/lib/lxc/$id/fstab
lxc.utsname = $id
lxc.arch = amd64

#network configuration
lxc.network.type = veth
lxc.network.flags = up
lxc.network.link = virbr0
lxc.network.ipv4 = 0.0.0.0/24
lxc.network.hwaddr = 00:1E:62:AA:AA:$(printf '%X\n' $((0xAA + $num)))
EOF
}

NUM=0

for i in n1 n2 n3 n4 n5; do
  buildbox $i $NUM
  NUM=$(($NUM + 1))
done


# configure virtual network bridge
cat > /tmp/default.xml <<EOF
<network>
  <name>default</name>
  <uuid>5329efc7-b33f-4585-86bf-da9f58952024</uuid>
  <forward mode='nat'>
    <nat>
      <port start='1024' end='65535'/>
    </nat>
  </forward>
  <bridge name='virbr0' stp='on' delay='0'/>
  <mac address='52:54:00:ba:ea:f4'/>
  <ip address='192.168.122.1' netmask='255.255.255.0'>
    <dhcp>
      <range start='192.168.122.11' end='192.168.122.100'/>
      <host mac='00:1E:62:AA:AA:AA' name='n1' ip='192.168.122.11'/>
      <host mac='00:1E:62:AA:AA:AB' name='n2' ip='192.168.122.12'/>
      <host mac='00:1E:62:AA:AA:AC' name='n3' ip='192.168.122.13'/>
      <host mac='00:1E:62:AA:AA:AD' name='n4' ip='192.168.122.14'/>
      <host mac='00:1E:62:AA:AA:AE' name='n5' ip='192.168.122.15'/>
    </dhcp>
  </ip>
</network>
EOF
virsh net-define /tmp/default.xml

virsh net-start default

# start all boxes
for i in n1 n2 n3 n4 n5; do
    lxc-start -n $i -d
done

