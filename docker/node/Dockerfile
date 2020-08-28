# Based on the deprecated `https://github.com/tutumcloud/tutum-debian`
FROM debian:buster

# Install packages
RUN apt-get -qy update && \
    apt-get -qy install \
        dos2unix \
        openssh-server \
        pwgen \
        && \
mkdir -p /var/run/sshd && \
sed -i "s/UsePrivilegeSeparation.*/UsePrivilegeSeparation no/g" /etc/ssh/sshd_config && \
sed -i "s/PermitRootLogin without-password/PermitRootLogin yes/g" /etc/ssh/sshd_config

ENV AUTHORIZED_KEYS **None**

ADD run.sh /run.sh
RUN dos2unix /run.sh \
    && chmod +x /*.sh

RUN apt-get update
RUN apt-get install -y apt-transport-https

# Install Jepsen deps
RUN apt-get install -qy build-essential bzip2 curl faketime iproute2 iptables iputils-ping libzip4 logrotate man man-db net-tools ntpdate psmisc python rsyslog sudo tar unzip vim wget

EXPOSE 22
CMD ["/run.sh"]
