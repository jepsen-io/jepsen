# How to set up nodes via LXC

#### [Mint 21.2 Victoria](#mint-212-victoria-1)
#### [Debian Bookworm - LXD](#debian-bookworm---lxd-1)
#### [Debian Bookworm - LXC](#debian-bookworm---lxc-1)

For further information, [LXD - Debian Wiki](https://wiki.debian.org/LXD).

----

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

----

## Debian Bookworm - LXD


#### Install host packages:
```bash
sudo apt install lxd lxd-tools dnsmasq-base btrfs-progs
```

#### Initialize LXD:
```bash
# defaults are good
sudo lxd init

# add yourself to the LXD group
sudo usermod -aG lxd <username>

# will need to logout/login for new group to be active

# try creating a sample container if you want
lxc launch images:debian/12 scratch
lxc list
lxc shell scratch
lxc stop scratch
lxc delete scratch
```

#### Create and start Jepsen's node containers:

```bash
for i in {1..10}; do lxc launch images:debian/12 n${i}; done
```

#### Configure LXD bridge network:

`lxd` automatically creates the bridge network, and `lxc launch` automatically configures the containers for it: 
```bash
lxc network list
+--------+----------+---------+----------------+---+-------------+---------+---------+
|  NAME  |   TYPE   | MANAGED |      IPV4      |...| DESCRIPTION | USED BY |  STATE  |
+--------+----------+---------+----------------+---+-------------+---------+---------+
| lxdbr0 | bridge   | YES     | 10.82.244.1/24 |...|             | 11      | CREATED |
+--------+----------+---------+----------------+---+-------------+---------+---------+
```

Assuming you are using `systemd-resolved`:

```bash
# confirm your settings
lxc network get lxdbr0 ipv4.address
lxc network get lxdbr0 ipv6.address
lxc network get lxdbr0 dns.domain    # will be blank if default lxd is used 

# create a systemd unit file
sudo nano /etc/systemd/system/lxd-dns-lxdbr0.service
# with the contents:
[Unit]
Description=LXD per-link DNS configuration for lxdbr0
BindsTo=sys-subsystem-net-devices-lxdbr0.device
After=sys-subsystem-net-devices-lxdbr0.device

[Service]
Type=oneshot
ExecStart=/usr/bin/resolvectl dns lxdbr0 10.82.244.1
ExecStart=/usr/bin/resolvectl domain lxdbr0 ~lxd
ExecStopPost=/usr/bin/resolvectl revert lxdbr0
RemainAfterExit=yes

[Install]
WantedBy=sys-subsystem-net-devices-lxdbr0.device

# bring up and confirm status
sudo systemctl daemon-reload
sudo systemctl enable --now lxd-dns-lxdbr0
sudo systemctl status lxd-dns-lxdbr0.service
sudo resolvectl status lxdbr0

ping n1
PING n1 (10.82.244.166) 56(84) bytes of data.
64 bytes from n1.lxd (10.82.244.166): icmp_seq=1 ttl=64 time=0.598 ms
```

#### Add required packages to node containers:

```bash
for i in {1..10}; do
  lxc exec n${i} -- sh -c "apt-get -qy update && apt-get -qy install openssh-server sudo";
done
```

#### Configure SSH:

Slip your preferred SSH key into each node's `.ssh/.authorized-keys`:
```bash
for i in {1..10}; do
  lxc exec n${i} -- sh -c "mkdir -p /root/.ssh && chmod 700 /root/.ssh/";
  lxc file push ~/.ssh/id_rsa.pub n${i}/root/.ssh/authorized_keys --uid 0 --gid 0 --mode 644;
done
```

Reset the root password to root, and allow root logins with passwords on each container.
If you've got an SSH agent set up, Jepsen can use that instead.
```bash
for i in {1..10}; do
  lxc exec n${i} -- bash -c 'echo -e "root\nroot\n" | passwd root';
  lxc exec n${i} -- sed -i 's,^#\?PermitRootLogin .*,PermitRootLogin yes,g' /etc/ssh/sshd_config;
  lxc exec n${i} -- systemctl restart sshd;
done
```

Store the node keys unencrypted so that jsch can use them.
If you already have the node keys, they may be unreadable to Jepsen -- remove them from `~/.ssh/known_hosts` and rescan:
```bash
for n in {1..10}; do
  ssh-keyscan -t rsa n${n} >> ~/.ssh/known_hosts;
done
```

#### Confirm that your host can ssh in:

```bash
ssh root@n1
```

#### Stopping and deleting containers:

```bash
for i in {1..10}; do
  lxc stop n${i};
  lxc delete n${i};
done
```

----

#### Real VMs w/Real Clocks

```bash
sudo apt install qemu-system

lxc launch images:debian/12 n1 --vm
```

Allows the clock nemesis to bump, skew, and scramble time in the Jepsen node as it's a real vm with a real clock.

----

#### Misc

The `lxc` command's \<Tab\> completion works well, even autocompletes container names.

#### LXD/LXC and Docker

There are issues with running LXD and Docker simultaneously, Docker grabs port forwarding.
Running Docker in an LXC container resolves the issue:
 [Prevent connectivity issues with LXD and Docker](https://documentation.ubuntu.com/lxd/en/latest/howto/network_bridge_firewalld/#prevent-connectivity-issues-with-lxd-and-docker).

----

## Debian Bookworm - LXC

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
