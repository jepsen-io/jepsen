# How to set up nodes via LXC

## Mint 21.2 Victoria

Install LXC

```sh
sudo apt install lxc lxc-templates libvirt-clients
```

Update the old GPG keys for debian releases

```sh
cd /tmp
wget "https://ftp-master.debian.org/keys/release-^C.asc"
sudo gpg --no-default-keyring --keyring=/var/cache/lxc/debian/archive-key.gpg --import release-12.asc
```

Set up a ZFS filesystem for containers. These are throwaway so I don't bother
with sync or atime.

```sh
sudo zfs create -o atime=off -o sync=disabled -o mountpoint=/var/lib/lxc rpool/lxc
```

If you've got Docker installed, it creates a whole bunch of firewall gunk that
totally breaks the LXC bridge. Make a script to let LXC talk:

```sh
sudo bash -c "cat >/usr/local/bin/post-docker.sh <<EOF
#!/usr/bin/env bash

date > /var/log/post-docker-timestamp
iptables -I DOCKER-USER -i lxcbr0 -j ACCEPT
iptables -I DOCKER-USER -o lxcbr0 -j ACCEPT
EOF"
sudo chmod +x /usr/local/bin/post-docker.sh
```

And call it after Docker loads:

```sh
sudo bash -c "cat >/etc/systemd/system/post-docker.service <<EOF
[Unit]
Description=Post Docker
After=docker.service
BindsTo=docker.service
ReloadPropagatedFrom=docker.service

[Service]
Type=oneshot
ExecStart=/usr/local/bin/post-docker.sh
ExecReload=/usr/local/bin/post-docker.sh
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF"
sudo systemctl daemon-reload
sudo systemctl enable post-docker.service
```

I think UFW might also interfere? See https://linuxcontainers.org/incus/docs/main/howto/network_bridge_firewalld/#prevent-connectivity-issues-with-incus-and-docker

```
sudo ufw allow in on lxcbr0
sudo ufw route allow in on lxcbr0
sudo ufw route allow out on lxcbr0
```

Create containers.

```sh
# To destroy: for i in {1..10}; do sudo lxc-destroy --force -n n$i; done
for i in {1..10}; do sudo lxc-create -n n$i -t debian -- --release bookworm; done
```

Uncomment LXC_DOMAIN in /etc/default/lxc-net

```sh
sudo vim /etc/default/lxc-net
```

And add the local dnsmasq to networkmanager

```sh
sudo bash -c "cat >/etc/NetworkManager/dnsmasq.d/lxc.conf <<EOF
server=/lxc/10.0.1.1
EOF"
```

Restart networking so that takes effect

```sh
sudo systemctl restart lxc-net
sudo systemctl restart NetworkManager
```

Set up resolved to reference the nodes. Check your interface name and address
in `ip addr`; they may vary.

```sh
sudo bash -c "cat >/etc/systemd/system/lxc-dns-lxcbr0.service <<EOF
[Unit]
Description=LXC DNS configuration for lxcbr0
After=lxc-net.service

[Service]
Type=oneshot
ExecStart=/usr/bin/resolvectl dns lxcbr0 10.0.1.1
ExecStart=/usr/bin/resolvectl domain lxcbr0 '~lxc'
ExecStopPost=/usr/bin/resolvectl revert lxcbr0
RemainAfterExit=yes

[Install]
WantedBy=lxc-net.service
EOF"
sudo systemctl daemon-reload
sudo systemctl enable lxc-dns-lxcbr0.service
sudo systemctl start lxc-dns-lxcbr0.service
```

Add the .lxc domain to resolved's search:

```sh
sudo vim /etc/systemd/resolved.conf
...
DOMAINS=lxc,...
```

Start nodes

```sh
for i in {1..10}; do
  sudo lxc-start -d -n n$i
done
```

Copy your SSH key to nodes and set their passwords to something trivial

```sh
export YOUR_SSH_KEY=~/.ssh/id_rsa.pub
for i in {1..10}; do
  sudo mkdir -p /var/lib/lxc/n${i}/rootfs/root/.ssh &&
  sudo chmod 700 /var/lib/lxc/n${i}/rootfs/root/.ssh/ &&
  sudo cp $YOUR_SSH_KEY /var/lib/lxc/n${i}/rootfs/root/.ssh/authorized_keys &&
  sudo chmod 644 /var/lib/lxc/n${i}/rootfs/root/.ssh/authorized_keys &&

  ## Set root password
  sudo lxc-attach -n n${i} -- bash -c 'echo -e "root\nroot\n" | passwd root';
  sudo lxc-attach -n n${i} -- sed -i 's,^#\?PermitRootLogin .*,PermitRootLogin yes,g' /etc/ssh/sshd_config;
  sudo lxc-attach -n n${i} -- systemctl restart sshd;
done
```

Install sudo on nodes; this is a base Jepsen dependency

```sh
for i in {1..10}; do
  sudo lxc-attach -n n${i} -- apt install -y sudo
done
```

Scan node SSH keys (as whatever user you'll run Jepsen as)

```sh
for n in {1..10}; do
  ssh-keyscan -t rsa n$n &&
  ssh-keyscan -t ed25519 n$n
done >> ~/.ssh/known_hosts
```

## Debian Bookworm

```sh
# Deps
sudo apt install -y lxc lxc-templates distro-info debootstrap bridge-utils libvirt-clients libvirt-daemon-system iptables ebtables dnsmasq-base libxml2-utils iproute2 bzip2 libnss-myhostname &&

# Create VMs
for i in {1..10}; do sudo lxc-create -n c$i -t debian -- --release bookworm; done &&

# Add network cards
for i in {1..10}; do
sudo bash -c "cat >>/var/lib/lxc/c${i}/config <<EOF

# Network config
lxc.net.0.type = veth
lxc.net.0.flags = up
lxc.net.0.link = virbr0
lxc.net.0.hwaddr = 00:1E:62:AA:AA:$(printf "%02x" $i)
EOF"
done &&

## Point the resolver at the local libvirt DNSmasq server. This used to go in
#/etc/dhcp/dhclient.conf and now systemd has done something horrible and
#dhclient still exists but doesn't seem to be used
sudo bash -c "cat >/etc/systemd/network/1-lxc-dns.network <<EOF
[Match]
Name=en*

[Network]
DHCP=yes
DNS=192.168.122.1
EOF" &&
sudo service systemd-networkd restart &&

# For some horrible inexplicable reason this results in dig/nslookup working
# fine but the system resolver still queries systemd-resolved which does NOT
# understand (why??? jfc) that we have this dnsmasq server--luckily we can
# bypass systemd's resolver at the nsswitch level altogether, jfc I hate
# systemd
sudo sed -i 's,resolve \[\!UNAVAIL=return\] ,,' /etc/nsswitch.conf &&

# Start nodes
sudo virsh net-autostart default &&
sudo virsh net-start default &&
for i in {1..10}; do
  sudo lxc-start -d -n c$i
done &&

## Create SSH key
ssh-keygen -b 2048 -t rsa -f ~/.ssh/lxc -q -N "" &&

## SSH setup
for i in {1..10}; do
  sudo mkdir -p /var/lib/lxc/c${i}/rootfs/root/.ssh &&
  sudo chmod 700 /var/lib/lxc/c${i}/rootfs/root/.ssh/ &&
  sudo cp /home/admin/.ssh/lxc.pub /var/lib/lxc/c${i}/rootfs/root/.ssh/authorized_keys &&
  sudo chmod 644 /var/lib/lxc/c${i}/rootfs/root/.ssh/authorized_keys &&

  ## Set root password
  sudo lxc-attach -n c${i} -- bash -c 'echo -e "root\nroot\n" | passwd root';
  sudo lxc-attach -n c${i} -- sed -i 's,^#\?PermitRootLogin .*,PermitRootLogin yes,g' /etc/ssh/sshd_config;
  sudo lxc-attach -n c${i} -- systemctl restart sshd;
done &&

## Install sudo
for i in {1..10}; do
  sudo lxc-attach -n c${i} -- apt install -y sudo
done &&

## Keyscan
for n in {1..10}; do ssh-keyscan -t rsa c$n; done >> ~/.ssh/known_hosts &&

## And SSH client config
{
cat >>~/.ssh/config <<EOF
# LXC DB nodes
Host c*
User root
IdentityFile ~/.ssh/lxc
EOF
} &&

## Create nodes file
for i in {1..10}; do
  echo "c$i" >> ~/lxc-nodes
done &&
```

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
