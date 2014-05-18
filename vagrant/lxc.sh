#!/bin/bash
# Setup lxc boxes to run tests

# basic packages
apt-get install -y lxc bridge-utils libvirt-bin debootstrap clusterssh dnsmasq

echo "cgroup  /sys/fs/cgroup  cgroup  defaults  0   0" >> /etc/fstab
mount /sys/fs/cgroup

lxc-checkconfig

# TODO real function...
function update_etc_hosts() {
    etchosts=$1

    cat >> $etchosts <<EOF
192.168.122.11 n1 n1.local
192.168.122.12 n2 n2.local
192.168.122.13 n3 n3.local
192.168.122.14 n4 n4.local
192.168.122.15 n5 n5.local
EOF
}

function buildbox() {
    id=$1
    num=$2

    container=/var/lib/lxc/$id

    if [[  -d $container ]]; then 
        echo "container $container already exists"  
        lxc-stop -n $i
    else
        echo "creating container $container"
        # create box - could clone after the first but it seems that debootstrap caches dloaded
        # stuff so creating is fast anyway
        SUITE=jessie MIRROR=http://ftp.fr.debian.org/debian lxc-create -n "$id" -t debian
    fi
    # update configuration, expecially mac address
    cat > "$container/config" <<EOF
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

    # TODO adjust number of hosts...
    update_etc_hosts "$container/rootfs/etc/hosts"

}

NUM=0

for i in n1 n2 n3 n4 n5; do
  buildbox $i $NUM
  NUM=$(($NUM + 1))
done

virsh net-destroy default
virsh net-undefine default

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

if virsh net-list | grep default > /dev/null 2>&1; then
    echo "virtual network default already started"
else
    echo "starting virtual network default"
    virsh net-start default
fi

update_etc_hosts /etc/hosts

function wait_container() {
    echo -n "waiting for $i to be up"
    while ! ping -c 1 $1 > /dev/null 2>&1 ;do
        echo -n .
        sleep 1
    done
    echo 
}

# start all boxes
for i in n1 n2 n3 n4 n5; do
    lxc-start -n $i -d

    wait_container $i

    # add host key to vagrant's known_hosts keys
    su vagrant -c "ssh-keygen -R $id"
    ssh-keyscan -H $i >> ~vagrant/.ssh/known_hosts

    container=/var/lib/lxc/$id

    # add vagrant's private key to hosts' root account authorized keys
    sshdir=$container/rootfs/root/.ssh
    [[ -d $sshdir ]] || mkdir -p $sshdir
    chmod 0700 $sshdir

    cat ~vagrant/.ssh/id_rsa.pub > $container/rootfs/root/.ssh/authorized_keys
done

