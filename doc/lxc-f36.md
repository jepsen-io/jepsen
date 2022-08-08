# How to set up nodes via LXC

## Fedora 36

As a user can be sudoer:

```
sudo dnf install -y openssh-server
sudo dnf -y install clusterssh
sudo dnf -y install dnsmasq
sudo dnf install lxc lxc-templates lxc-extra debootstrap libvirt perl gpg
sudo dnf -y install bridge-utils libvirt virt-install qemu-kvm
sudo dnf install libvirt-devel virt-top libguestfs-tools guestfs-tools
```

Then, run all ``systemctl start/enable`` as you need:

```
sudo systemctl start sshd.service
sudo systemctl enable sshd.service

sudo systemctl start lxc.service
sudo systemctl enable lxc.service

sudo systemctl start  libvirtd.service
sudo systemctl enable  libvirtd.service

sudo systemctl start  dnsmasq.service
sudo systemctl enable  dnsmasq.service



<...> so on <..>
```
Watch out libvirtd and kvm messages... 

Apply google and kernel parameters until checkconfig passes:

```
lxc-checkconfig
```

Well, here we go, problems are comming..

```
[root@fedora ~]# lxc-checkconfig
LXC version 4.0.12
Kernel configuration not found at /proc/config.gz; searching...
Kernel configuration found at /boot/config-5.18.16-200.fc36.x86_64
--- Namespaces ---
Namespaces: enabled
Utsname namespace: enabled
Ipc namespace: enabled
Pid namespace: enabled
User namespace: enabled
Warning: newuidmap is not setuid-root
Warning: newgidmap is not setuid-root
Network namespace: enabled

--- Control groups ---
Cgroups: enabled
Cgroup namespace: enabled

Cgroup v1 mount points:


Cgroup v2 mount points:
/sys/fs/cgroup

Cgroup v1 systemd controller: missing
Cgroup v1 freezer controller: missing
Cgroup ns_cgroup: required
Cgroup device: enabled
Cgroup sched: enabled
Cgroup cpu account: enabled
Cgroup memory controller: enabled
Cgroup cpuset: enabled

--- Misc ---
Veth pair device: enabled, loaded
Macvlan: enabled, not loaded
Vlan: enabled, not loaded
Bridges: enabled, loaded
Advanced netfilter: enabled, not loaded
CONFIG_IP_NF_TARGET_MASQUERADE: enabled, not loaded
CONFIG_IP6_NF_TARGET_MASQUERADE: enabled, not loaded
CONFIG_NETFILTER_XT_TARGET_CHECKSUM: enabled, loaded
CONFIG_NETFILTER_XT_MATCH_COMMENT: enabled, not loaded
FUSE (for use with lxcfs): enabled, loaded

--- Checkpoint/Restore ---
checkpoint restore: enabled
CONFIG_FHANDLE: enabled
CONFIG_EVENTFD: enabled
CONFIG_EPOLL: enabled
CONFIG_UNIX_DIAG: enabled
CONFIG_INET_DIAG: enabled
CONFIG_PACKET_DIAG: enabled
CONFIG_NETLINK_DIAG: enabled
File capabilities:

Note : Before booting a new kernel, you can check its configuration
usage : CONFIG=/path/to/config /usr/bin/lxc-checkconfig
```

Ok, 

``Cgroup ns_cgroup: required``  forget about it, it is problem about versions...

Fedora 36 use as default cgroups2, if you are running v1, sorry, and disable selinux, it is just testing...

``cgroup_no_v1=all selinux=0``


 Create VMs arch amd64, 3 of them :

```
for i in {1..3}; do sudo lxc-create -t download -n n$i -- -d fedora -r 36 -a amd64; done
```

Ok, add network configuration to each node. We assign each a sequential MAC
address.

Right, but you need known where you are, ``brctl show ``

```
bridge name     bridge id               STP enabled     interfaces
docker0         8000.02425aa0dc72       no
lxcbr0          8000.00163e000000       no               
virbr0          8000.525400bb5910       yes             vethDcihoW
```
ok, ``virbr0`` seems our winner ...

Then 
```
for i in {1..3}; do
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
for i in {1..3}; do
  virsh net-update --current default add-last ip-dhcp-host "<host mac=\"00:1E:62:AA:AA:$(printf "%02x" $i)\" name=\"n${i}\" ip=\"192.168.122.1$(printf "%02d" $i)\"/>"
done
```

Start the network, and set it to start at boot so the dnsmasq will be
available.

Well, we need know what libvirtd service is doing ...

```
[root@fedora ~]# systemctl status libvirtd
○ libvirtd.service - Virtualization daemon
     Loaded: loaded (/usr/lib/systemd/system/libvirtd.service; disabled; vendor preset: disabled)
     Active: inactive (dead) since Mon 2022-08-08 09:10:49 UTC; 1h 19min ago
TriggeredBy: ● libvirtd-ro.socket
             ○ libvirtd-tls.socket
             ○ libvirtd-tcp.socket
             ● libvirtd-admin.socket
             ● libvirtd.socket
       Docs: man:libvirtd(8)
             https://libvirt.org
    Process: 2678 ExecStart=/usr/sbin/libvirtd $LIBVIRTD_ARGS (code=exited, s
```

Then, next steps
```
[root@fedora ~]# virsh net-autostart default;
Network default marked as autostarted
```
So, continue,
```
virsh net-start default
```
you get,

```
error: Failed to start network default
error: Requested operation is not valid: network is already active
```

fedora 36,  libvirtd.socket is managing  libvirtd-tls.socket and libvirtd-tcp.socket ... 

If you configure resolv.conf by hand, add the libvirt local dnsmasq to
resolv.conf:

```
echo -e "nameserver 192.168.122.1\n$(cat /etc/resolv.conf)" > /etc/resolv.conf

```

If you're letting dhclient manage it, then:

```
echo "prepend domain-name-servers 192.168.122.1;" >>/etc/dhcp/dhclient.conf
systemctl restart NetworkManager

```

And update ``/etc/hosts`` if you don't wanna lose your mind seeing ``ping`` fails...

```
192.168.122.101 n1
192.168.122.102 n2
192.168.122.103 n3
```



Slip your preferred SSH key into each node's `.authorized-keys`:

```
for i in {1..3}; do
  mkdir -p /var/lib/lxc/n${i}/rootfs/root/.ssh
  chmod 700 /var/lib/lxc/n${i}/rootfs/root/.ssh/
  cp ~/.ssh/id_rsa.pub /var/lib/lxc/n${i}/rootfs/root/.ssh/authorized_keys
  chmod 644 /var/lib/lxc/n${i}/rootfs/root/.ssh/authorized_keys
done
```

And start the nodes:

```
for i in {1..3}; do
  lxc-start -d -n n$i
done
```

To stop them:

```
for i in {1..3}; do
  lxc-stop -n n$i
done
```

To check them:

```
lxc-ls -f

NAME      STATE   AUTOSTART GROUPS IPV4            IPV6 UNPRIVILEGED

n1        RUNNING 0         -      192.168.122.101 -    false
n2        RUNNING 0         -      192.168.122.102 -    false
n3        RUNNING 0         -      192.168.122.103 -    false
```


Reset the root passwords to whatever you like. Jepsen uses `root` by default,
and allow root logins with passwords on each container. If you've got an SSH
agent set up, Jepsen can use that instead.

<b>Warning !!!</b>
Be sure that sshd service is running -ok- in {n1,n2,n3} without problems ... 
``lxc-attach -n {n1, n2, n3} ``

Be sure that you change <u> "sshd_config" </u>

```
for i in {1..3}; do
  lxc-attach -n n${i} -- bash -c 'echo -e "root\nroot\n" | passwd root';
  lxc-attach -n n${i} -- sed -i 's,^#\?PermitRootLogin .*,PermitRootLogin yes,g' /etc/ssh/sshd_config;
  lxc-attach -n n${i} -- systemctl restart sshd;
done
```

Store the host keys unencrypted so that jsch can use them. If you already have
the host keys, they may be unreadable to Jepsen--remove them from .known_hosts
and rescan.

```
for n in {1..3}; do ssh-keyscan -vvv -t rsa n$n; done >> ~/.ssh/known_hosts
```
Check it:

```
[root@fedora ~]# cat .ssh/known_hosts
n1 ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQCzqHvG5GEmX8RaALxTZT22fX1hDsxljAPH/m/6=
n2 ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQC+taCDJSyJbu5oaRK/zTFu+CvqOx0MRkvzXfoM=
n3 ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQDcpQcG9GUjYZqM5dA5LwSoCH6Qot42mbsdXwva=
```

If you get some problems as "write failed or similars", check again sshd, you got some wrong...


And check that you can SSH to the nodes:

fedora 36 needs,

```

 dnf -y install clusterssh
```

If you don't have X mode, graphical mode... running

```
cssh n1 n2 n3 
```
you should be sometime similar :

```
[root@fedora ~]# cssh n1
Can't connect to display `localhost:0': Connection refused at /usr/share/perl5/vendor_perl/X11/Protocol.pm line 2269.
``` 

Not use cssh, use ssh in a loop.

--- O --- 

