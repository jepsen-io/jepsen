# See https://github.com/jgoerzen/docker-debian-base
# See https://hub.docker.com/r/jgoerzen/debian-base-standard
FROM jgoerzen/debian-base-standard:stretch

# I think this is a bug--debian-base-setup crashes because policy-rc.d isn't
# present in this image, and if you create it, exim crashes... do we actually NEED this? Maybe not...
#RUN mkdir /usr/sbin/policy-rc.d
#RUN run-parts --exit-on-error --verbose /usr/local/debian-base-setup

RUN apt-get update

# Basic system stuff
RUN apt-get install -y apt-transport-https

# Install packages
RUN apt-get -qy update && \
    apt-get -qy install \
        dos2unix \
        openssh-server \
        pwgen

# When run, boot-debian-base will call this script, which will copy the
# authorized_keys file into root's .ssh
ADD setup-authorized-keys.sh /usr/local/preinit/03-setup-authorized-keys
RUN chmod +x /usr/local/preinit/03-setup-authorized-keys

# Configure SSHD
RUN sed -i "s/#PermitRootLogin prohibit-password/PermitRootLogin yes/g" /etc/ssh/sshd_config

# Enable SSH server
ENV DEBBASE_SSH enabled

# Install Jecci deps
RUN apt-get install -qy build-essential bzip2 curl faketime iproute2 \
    iptables iputils-ping libzip4 logrotate man man-db net-tools ntpdate \
    psmisc python rsyslog sudo tar unzip vim wget ca-certificates

EXPOSE 22

# For some db like postgres, starting the db as root is not allowed
# So add a user here
RUN groupadd jecci &&\
    useradd -m jecci -g jecci -s /bin/bash &&\
    echo "jecci:123456" | /usr/sbin/chpasswd && chage -M 99999 jecci &&\
    echo "jecci ALL=(ALL) NOPASSWD:ALL" >>/etc/sudoers &&\
    mkdir -p /home/jecci/.ssh

CMD ["/usr/local/bin/boot-debian-base"]