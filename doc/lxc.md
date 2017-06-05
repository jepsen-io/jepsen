# How to set up nodes via LXC
## Debian Testing:

(refer to https://wiki.debian.org/LXC)

```sh
aptitude install lxc bridge-utils ebtables libvirt-bin debootstrap dnsmasq
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
lxc-create -n n1 -t debian -- --release jessie
lxc-create -n n2 -t debian -- --release jessie
lxc-create -n n3 -t debian -- --release jessie
lxc-create -n n4 -t debian -- --release jessie
lxc-create -n n5 -t debian -- --release jessie
```

Note the root passwords.

Edit /var/lib/lxc/n1/config and friends, changing the network hwaddr to something unique. I suggest using sequential mac addresses for n1, n2, n3, ....

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

# Stuff to add:
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

Drop an entry in `/etc/resolv.conf` to read from the libvirt network dns:

```
nameserver 192.168.122.1  # Local libvirt dnsmasq
nameserver 192.168.1.1    # Regular network resolver
```

Kill the system default dnsmasq (if you have one), and start the network (which
in turn will start a replacement dnsmasq with the LXC config. Then, start up
all the nodes. I have this in a bash script called `jepsen-start`:

```sh
#!/bin/sh
sudo service dnsmasq stop
sudo virsh net-start default
sudo lxc-start -d -n n1
sudo lxc-start -d -n n2
sudo lxc-start -d -n n3
sudo lxc-start -d -n n4
sudo lxc-start -d -n n5
```

Fire up each VM:

```sh
jepsen-start
```

Log into the containers, (may have to specify tty 0 to use console correctly) e.g.,:

```sh
lxc-console --name n1 -t 0
```

(Optional?) In the containers, update keys used by apt to verify packages:

```sh
apt-key update
apt-get update
```

And set your root password--I use `root`/`root` by default in Jepsen.

```sh
passwd
```

Copy your SSH key (on host):

```sh
cat ~/.ssh/id_rsa.pub
```

and add it to root's `authorized_keys` (in containers):

```sh
apt-get install -y sudo vim
mkdir ~/.ssh
chmod 700 ~/.ssh
touch ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
vim ~/.ssh/authorized_keys
```

Enable password-based login for root (used by jsch):
```sh
sed  -i 's,^PermitRootLogin .*,PermitRootLogin yes,g' /etc/ssh/sshd_config
systemctl restart sshd
apt install sudo
```

[Remove systemd](http://without-systemd.org/wiki/index.php/How_to_remove_systemd_from_a_Debian_jessie/sid_installation). After you install sysvinit-core and sysvinit-utils, you may have to restart the container with /lib/sysvinit/init argument to lxc-start before apt will allow you to remove systemd.

Detach from the container with Control+a q, and repeat for the remaining nodes.

On the control node, drop entries in `~/.ssh/config` for nodes:

```
Host n*
User root
```

Store the host keys unencrypted so that jsch can use them. If you already have
the host keys, they may be unreadable to Jepsen--remove them from .known_hosts
and rescan.

```
for n in $(seq 1 5); do ssh-keyscan -t rsa n$n; done >> ~/.ssh/known_hosts
```

And check that you can SSH to the nodes

```sh
cssh n1 n2 n3 n4 n5
```

And that should mostly do it, I think.

## Ubuntu 14.04 / trusty

Follow generally the same steps as for Debian, but the process is easier. Reference: https://help.ubuntu.com/lts/serverguide/lxc.html

* Right after you have installed LXC, create or open /etc/lxc/dnsmasq.conf and add the following contents:

```
dhcp-host=n1,10.0.3.101
dhcp-host=n2,10.0.3.102
dhcp-host=n3,10.0.3.103
dhcp-host=n4,10.0.3.104
dhcp-host=n5,10.0.3.105
```

10.0.3.* is LXC's default network. If you want others, go for it but you'll have to change it in the main configuration for lxc as well.

* you may not need to add cgroup to fstab and/or mount it. /sys/fs/cgroups may already be there.
* Then, go and run the lxc-create command, but...
* no need to edit /var/lib/lxc/*/config or set up a bridge, LXC does that for you.
* Fire up the boxes (lxc-start -n n{1,2,3,4,5} -d) and you should be able to ssh right into them.
* Follow the rest of the Debian tutorial, but make sure to use the correct ip addresses.
