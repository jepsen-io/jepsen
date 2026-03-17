# How to set up nodes via LXC

#### [Mint 22.3 Zena](#mint-22.3-zena-1)
#### [Debian 13/trixie - Incus](#debian-13trixie---incus-1)

For further information, [LXD - Debian Wiki](https://wiki.debian.org/LXD).

----

## Mint 22.3 Zena

Install LXC and DNSMasq:

```sh
sudo apt install lxc lxc-templates libvirt-clients dnsmasq
```

Update the old GPG keys for debian releases

```sh
cd /tmp
wget "https://ftp-master.debian.org/keys/archive-key-13.asc"
sudo gpg --no-default-keyring --keyring=/etc/apt/trusted.gpg.d/debian-archive-trixie-stable.gpg --import archive-key-13.asc
```

Set up a ZFS filesystem for containers. These are throwaway so I don't bother
with sync or atime.

```sh
sudo zfs create -o acltype=posix -o atime=off -o sync=disabled -o mountpoint=/var/lib/lxc rpool/lxc
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
for i in {1..10}; do sudo lxc-create -n n$i -t debian -- --release trixie; done
```

Uncomment this line in `/etc/default/lxc-net` to allow DHCP address reservations:

```sh
LXC_DHCP_CONFILE=/etc/dnsmasq.conf
```

Uncomment `bind-interfaces` in `/etc/dnsmasq.conf`, because otherwise
systemd-resolved will fight it. Also uncomment `conf-dir`:

```
bind-interfaces
conf-dir=/etc/dnsmasq.d
```

But disable the actual dnsmasq service, because lxc-net will run it.

```
sudo systemctl stop dnsmasq
sudo systemctl disable dnsmasq
```

Disable the systemd-resolved stub listener in `/etc/systemd/resolved.conf`, and
search the .lxc domain. Ignore resolv.conf, and instead hardcode your preferred
upstream DNS resolver (mine is 10.0.0.1).

```
...
DNSStubListener=no
DOMAINS=lxc,...
no-resolv
server=10.0.0.1
```

Add DHCP reservations for the nodes. The 10.0.3.xxx here should line up with
the network address on `lxcbr0`; check `ifconfig lxcbr0`.

```
for i in {1..10}; do sudo bash -c "echo 'dhcp-host=n$i,10.0.3.1$(printf "%02d" $i)' >> /etc/dnsmasq.d/jepsen.conf"; done
```

Restart the resolver and LXC networking

```
sudo systemctl restart systemd-resolved
sudo systemctl restart lxc-net
```

Start up a container and confirm that it's assigned the right address:

```
sudo lxc-start n1
sudo lxc-ls --fancy
sudo lxc-stop n1
```

And add the local dnsmasq to networkmanager

```sh
sudo bash -c "cat >/etc/NetworkManager/dnsmasq.d/lxc.conf <<EOF
server=/lxc/10.0.3.1
EOF"
```

Insist that NetworkManager use dnsmasq, *not* the public DNS. Get the long UUID
here from `nmcli con`.

```sh
sudo nmcli con mod d41034c4-48d0-3867-922c-73480603ff2e ipv4.ignore-auto-dns yes
sudo nmcli con mod d41034c4-48d0-3867-922c-73480603ff2e ipv4.dns "10.0.3.1"
```

Restart networking so that takes effect, and/or bounce the interface

```sh
sudo systemctl restart NetworkManager
sudo nmcli con down d41034c4-48d0-3867-922c-73480603ff2e
sudo nmcli con up d41034c4-48d0-3867-922c-73480603ff2e
```

At this juncture `cat /etc/resolv.conf` should show only 10.0.3.1, the local
dnsmasq. Dig `google.com` should still resolve using your upstream resolver.

Set up resolved to reference the nodes. Check your interface name and address
in `ip addr`; they may vary. This miiiight be optional with the above nmcli
settings.

```sh
sudo bash -c "cat >/etc/systemd/system/lxc-dns-lxcbr0.service <<EOF
[Unit]
Description=LXC DNS configuration for lxcbr0
After=lxc-net.service

[Service]
Type=oneshot
ExecStart=/usr/bin/resolvectl dns lxcbr0 10.0.3.1
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

Start nodes

```sh
for i in {1..10}; do
  sudo lxc-start -d -n n$i
done
```

[This](https://thelinuxcode.com/how-to-configure-dns-on-linux-2026-systemd-resolved-networkmanager-resolvconf-and-bind/)
is a helpful guide to fixing resolv.conf/resolved/dnsmasq/NetworkManager
issues. You may need to fully restart too.

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

At this point you should be able to `ssh n1` without a password.

----

## Debian 13/trixie - Incus

Due to Canonical's re-licensing and imposing of a CLA, the last version of Debian to include LXD will be trixie. Users are encouraged to migrate to Incus after upgrading from bookworm to trixie.

### Install Host Packages

```bash
sudo apt update && sudo apt install incus systemd-resolved
```

### Initialize Incus

```bash
# add yourself to the incus-admin group to avoid having to be root or sudo
# you will need to logout/login for new group to be active
sudo adduser $USER incus-admin

# initialize Incus with default config, defaults are usually OK
incus admin init --minimal

# try creating a sample container if you want
incus launch images:debian/13 scratch
incus list
incus shell scratch
incus stop scratch
incus delete scratch
```

### Create and Start Jepsen's Node Containers

```bash
for i in {1..10}; do
  incus launch images:debian/13 n${i};
done
```

### Confirm Incus' Bridge Network

`incus init` automatically created the bridge network, and `incus launch` automatically configured the containers for it:

```bash
incus network list
+----------+----------+---------+----------------+---+-------------+---------+---------+
|  NAME    |   TYPE   | MANAGED |      IPV4      |...| DESCRIPTION | USED BY |  STATE  |
+----------+----------+---------+----------------+---+-------------+---------+---------+
| incusbr0 | bridge   | YES     | 10.242.68.1/24 |...|             | 11      | CREATED |
+----------+----------+---------+----------------+---+-------------+---------+---------+

# confirm your settings
incus network get incusbr0 ipv4.address
incus network get incusbr0 ipv6.address
incus network get incusbr0 dns.domain    # will be blank if default incus config is used 

# confirm containers are reachable
ping n1
PING n1 (10.242.68.40) 56(84) bytes of data.
64 bytes from n1 (10.242.68.40): icmp_seq=1 ttl=64 time=0.030 ms
...
```

If you want to install and run Docker, it may mess up your networking and firewall.  If your Incus containers are not reachable from the host, see the Incus documentation:

- [Prevent connectivity issues with Incus and Docker](https://linuxcontainers.org/incus/docs/main/howto/network_bridge_firewalld/#prevent-connectivity-issues-with-incus-and-docker)

#### Add Required Packages to Node Containers

```bash
for i in {1..10}; do
  incus exec n${i} -- sh -c "apt-get -qy update && apt-get -qy install openssh-server sudo";
done
```

#### Configure SSH

Slip your preferred SSH key into each node's `.ssh/.authorized-keys`:

```bash
for i in {1..10}; do
  incus exec n${i} -- sh -c "mkdir -p /root/.ssh && chmod 700 /root/.ssh/";
  incus file push ~/.ssh/id_rsa.pub n${i}/root/.ssh/authorized_keys --uid 0 --gid 0 --mode 644;
done
```

Reset the root password to root, and allow root logins with passwords on each container.
If you've got an SSH agent set up, Jepsen can use that instead.

```bash
for i in {1..10}; do
  incus exec n${i} -- bash -c 'echo -e "root\nroot\n" | passwd root';
  incus exec n${i} -- sed -i 's,^#\?PermitRootLogin .*,PermitRootLogin yes,g' /etc/ssh/sshd_config;
  incus exec n${i} -- systemctl restart sshd;
done
```

Store the node keys unencrypted so that jsch can use them.
If you already have the node keys, they may be unreadable to Jepsen -- remove them from `~/.ssh/known_hosts` and rescan:

```bash
for n in {1..10}; do
  ssh-keyscan -t rsa n${n} >> ~/.ssh/known_hosts;
done
```

#### Confirm You Can `ssh` Into Nodes

```bash
ssh root@n1
```

#### Stopping and Deleting Containers

```bash
for i in {1..10}; do
  incus stop n${i} --force;
  incus delete n${i} --force;
done
```

----

#### Real VMs w/Real Clocks

```bash
# VM's use QEMU
sudo apt update && sudo apt install qemu-system

# note --vm flag
incus launch images:debian/13 n1 --vm
```

Allows the clock nemesis to bump, skew, and scramble time in a Jepsen node as it's a real vm with a real clock.

----

#### Misc

The `incus` command's \<Tab\> completion works well, even autocompletes container names.
