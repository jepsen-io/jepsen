FROM debian:stretch
MAINTAINER jake@apache.org

ENV LEIN_ROOT true
ENV DEBIAN_FRONTEND=noninteractive

RUN cat /etc/apt/sources.list
RUN apt -q -y update

# to fix bugs when installing openjdk-11-jdk-headless
# here we need to install jdk first
RUN apt install -qy openjdk-11-jdk

#
# Jecci dependencies
#
RUN apt install -qy vim \
    emacs \
    git \
    htop \
    screen \
    pssh \
    curl \
    wget \
    gnuplot \
    graphviz \
    dos2unix \
    openjdk-11-jdk-headless \
    libjna-java \
    libssl-dev \
    #for rhizome
    xserver-xorg-video-dummy

RUN wget https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein && \
   mv lein /usr/bin && \
   chmod +x /usr/bin/lein && \
   mkdir -p $HOME/.lein

COPY xorg.conf /etc/X11/xorg.conf
ENV DISPLAY=:0

# without --dev flag up.sh copies jecci to these subfolders
# with --dev flag they are empty until mounted
COPY jecci/jecci /jecci/jecci/
# ADD the following to change lein source, if you are in... you know...
# ADD ./profiles.clj /root/.lein/profiles.clj
RUN cat /root/.lein/profiles.clj &&\
    if [ -f /jecci/jecci/project.clj ]; then cd /jecci/jecci && lein install; fi
COPY jecci /jecci/

ADD ./bashrc /root/.bashrc
ADD ./init.sh /init.sh
RUN dos2unix /init.sh /root/.bashrc \
    && chmod +x /init.sh

# fix trustAnchors parameter must be non-empty bug
RUN /usr/bin/printf '\xfe\xed\xfe\xed\x00\x00\x00\x02\x00\x00\x00\x00\xe2\x68\x6e\x45\xfb\x43\xdf\xa4\xd9\x92\xdd\x41\xce\xb6\xb2\x1c\x63\x30\xd7\x92' > /etc/ssl/certs/java/cacerts
RUN /var/lib/dpkg/info/ca-certificates-java.postinst configure

RUN echo "root:123456" | /usr/sbin/chpasswd && chage -M 99999 root

RUN groupadd jecci &&\
    useradd -m jecci -g jecci -s /bin/bash &&\
    echo "jecci:123456" | /usr/sbin/chpasswd && chage -M 99999 jecci &&\
    echo "jecci ALL=(ALL) NOPASSWD:ALL" >>/etc/sudoers &&\
    mkdir -p /home/jecci/.ssh

# for enabling vscode debug
RUN apt install -qy openssh-server
ENV DEBBASE_SSH enabled
EXPOSE 22

CMD /init.sh