#!/bin/bash
# setup virtual network default for five hosts named n1 through n5


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

