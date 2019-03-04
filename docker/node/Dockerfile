# A mashup of tutum:debian and rastasheep/ubuntu-sshd
FROM       ubuntu:18.04

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update
RUN apt-get install -y openssh-server \
    curl faketime iproute2 iptables iputils-ping libzip4 \
    logrotate man man-db net-tools ntpdate psmisc python rsyslog \
    sudo unzip vim wget apt-transport-https \
    && apt-get remove -y --purge --auto-remove systemd

RUN mkdir /var/run/sshd

RUN echo 'root:root' |chpasswd

RUN sed -ri 's/^#?PermitRootLogin\s+.*/PermitRootLogin yes/' /etc/ssh/sshd_config
RUN sed -ri 's/^#?UsePAM\s+.*/UsePAM no/' /etc/ssh/sshd_config

RUN mkdir /root/.ssh

RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

ENV AUTHORIZED_KEYS **None**
ADD run.sh /run.sh
RUN chmod +x /*.sh

EXPOSE 22

CMD ["/run.sh"]
