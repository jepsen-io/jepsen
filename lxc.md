# How to set up nodes via LXC
## Debian Testing:

(refer to https://wiki.debian.org/LXC)

```sh
aptitude install lxc bridge-utils libvirt-bin debootstrap dnsmasq
```

Add this line to /etc/fstab:

```
cgroup  /sys/fs/cgroup  cgroup  defaults  0   0
```

Mount ze cgroup

```
mount /sys/fs/cgroup
```

Apply google and kernel parameters until checkconfig passes:

```
lxc-checkconfig
```

Create a VM or five

```
SUITE=jessie MIRROR=http://ftp.fr.debian.org/debian lxc-create -n n1 -t debian
```

Edit /var/lib/lxc/n1/config, changing the network hwaddr to something unique. I suggest using sequential mac addresses for n1, n2, n3, ....

```
# Template used to create this container: /usr/share/lxc/templates/lxc-debian
# Parameters passed to the template:
# For additional config options, please look at lxc.conf(5)

lxc.rootfs = /var/lib/lxc/n1/rootfs

# Common configuration
lxc.include = /usr/share/lxc/config/debian.common.conf

# Container specific configuration
lxc.mount = /var/lib/lxc/n1/fstab
lxc.utsname = n1
lxc.arch = amd64

lxc.network.type = veth
lxc.network.flags = up
lxc.network.link = virbr0
lxc.network.ipv4 = 0.0.0.0/24
lxc.network.hwaddr = 00:1E:62:AA:AA:AA
```

Set up libvirt network, and assign MAC->IP bindings for the LXC node mac addrs

```sh
virsh net-edit default
```

```xml
<network>
  <name>default</name>
  <uuid>08063db9-38f4-4c9c-8887-08000f13ce80</uuid>
  <forward mode='nat'/>
  <bridge name='virbr0' stp='on' delay='0'/>
  <mac address='52:54:00:8e:29:d2'/>
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
```

Start network

```sh
virsh net-start default
```

Drop entries in `~/.ssh/config` for nodes:

```
Host n*
User root
```

Log into every box. LXC mentions the password at container creation; usually
`root`.

```sh
cssh n1 n2 n3 n4 n5
```

Set up hostfiles on each box with hardcoded IP addresses


```
127.0.0.1 localhost n1 n1.local
::1       localhost ip6-localhost ip6-loopback
fe00::0   ip6-localnet
ff00::0   ip6-mcastprefix
ff02::1   ip6-allnodes
ff02::2   ip6-allrouters

192.168.122.11 n1 n1.local
192.168.122.12 n2 n2.local
192.168.122.13 n3 n3.local
192.168.122.14 n4 n4.local
192.168.122.15 n5 n5.local
```

And that should mostly do it, I think.

## Ubuntu 14.04 / trusty
Follow generally the same steps as for Debian, but the process is easier. Reference: https://help.ubuntu.com/lts/serverguide/lxc.html
* right after you have installed LXC, create or open /etc/lxm/dnsmasq.com and add the following contents:

  ```
dhcp-host=n1,10.0.3.101
dhcp-host=n2,10.0.3.102
dhcp-host=n3,10.0.3.103
dhcp-host=n4,10.0.3.104
dhcp-host=n5,10.0.3.105
  ```
  10.0.3.* is LXM's default network. If you want others, go for it but you'll have to change it in the main configuration for lxm as well.
* you may not need to add cgroup to fstab and/or mount it. /sys/fs/cgroups may already be there.
* Then, go and run the lxc-create command, but...
* no need to edit /var/lib/lxc/*/config or set up a bridge, LXC does that for you.
* Fire up the boxes (lxc-start -n n{1,2,3,4,5} -d) and you should be able to ssh right into them.
* Follow the rest of the Debian tutorial, but make sure to use the correct ip addresses.
