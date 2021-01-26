# How to set up nodes via LXC

## Debian Buster

(Refer to https://wiki.debian.org/LXC)

```
apt install lxc debootstrap bridge-utils libvirt-clients libvirt-daemon-system iptables ebtables dnsmasq-base libxml2-utils iproute2
```

Apply google and kernel parameters until checkconfig passes:

```
lxc-checkconfig
```

Create VMs:

```
for i in {1..10}; do sudo lxc-create -n n$i -t debian -- --release buster; done
```

Add network configuration to each node. We assign each a sequential MAC
address.

```
for i in {1..10}; do
sudo cat >>/var/lib/lxc/n${i}/config <<EOF

# Network config
lxc.net.0.type = veth
lxc.net.0.flags = up
lxc.net.0.link = virbr0
lxc.net.0.hwaddr = 00:1E:62:AA:AA:$(printf "%02x" $i)
EOF
done
```

Set up the virsh network bindings mapping those MAC addresses to hostnames and
IP addresses:

```
for i in {1..10}; do
  virsh net-update --current default add-last ip-dhcp-host "<host mac=\"00:1E:62:AA:AA:$(printf "%02x" $i)\" name=\"n${i}\" ip=\"192.168.122.1$(printf "%02d" $i)\"/>"
done
```

Start the network, and set it to start at boot so the dnsmasq will be
available.

```
virsh net-autostart default;
virsh net-start default
```

If you configure resolv.conf by hand, add the libvirt local dnsmasq to
resolv.conf:

```
echo -e "nameserver 192.168.122.1\n$(cat /etc/resolv.conf)" > /etc/resolv.conf
```

If you're letting dhclient manage it, then:

```
echo "prepend domain-name-servers 192.168.122.1;" >>/etc/dhcp/dhclient.conf
sudo service networking restart
```

Slip your preferred SSH key into each node's `.authorized-keys`:

```
for i in {1..10}; do
  mkdir -p /var/lib/lxc/n${i}/rootfs/root/.ssh
  chmod 700 /var/lib/lxc/n${i}/rootfs/root/.ssh/
  cp ~/.ssh/id_rsa.pub /var/lib/lxc/n${i}/rootfs/root/.ssh/authorized_keys
  chmod 644 /var/lib/lxc/n${i}/rootfs/root/.ssh/authorized_keys
done
```

And start the nodes:

```
for i in {1..10}; do
  lxc-start -d -n n$i
done
```

To stop them:

```
for i in {1..10}; do
  lxc-stop -n n$i
done
```

Reset the root passwords to whatever you like. Jepsen uses `root` by default,
and allow root logins with passwords on each container. If you've got an SSH
agent set up, Jepsen can use that instead.

```
for i in {1..10}; do
  lxc-attach -n n${i} -- bash -c 'echo -e "root\nroot\n" | passwd root';
  lxc-attach -n n${i} -- sed -i 's,^#\?PermitRootLogin .*,PermitRootLogin yes,g' /etc/ssh/sshd_config;
  lxc-attach -n n${i} -- systemctl restart sshd;
done
```

Store the host keys unencrypted so that jsch can use them. If you already have
the host keys, they may be unreadable to Jepsen--remove them from .known_hosts
and rescan.

```
for n in {1..10}; do ssh-keyscan -t rsa n$n; done >> ~/.ssh/known_hosts
```

Install `sudo`:

```sh
for i in {1..10}; do
  lxc-attach -n n${i} -- apt install -y sudo
done
```

And check that you can SSH to the nodes

```sh
cssh n1 n2 n3 n4 n5
```

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
