FROM ubuntu:16.04
MAINTAINER jake@apache.org

ENV LEIN_ROOT true

#
# Jepsen dependencies
#
RUN apt-get -y -q update && \
    apt-get -y -q install software-properties-common && \
    add-apt-repository ppa:openjdk-r/ppa && \
    apt-get -y -q update && \
    apt-get install -qqy \
        openjdk-8-jdk \
        libjna-java \
        git \
        gnuplot \
        wget \
	vim # not required by jepsen itself, just for ease of use

RUN wget https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein && \
    mv lein /usr/bin && \
    chmod +x /usr/bin/lein && \
    lein self-install

ARG uid
ARG gid

RUN groupadd -g $gid jepsen \
	&& useradd -m -u $uid -g jepsen jepsen
USER jepsen

# without --dev flag up.sh copies jepsen to these subfolders
# with --dev flag they are empty until mounted
COPY jepsen/jepsen /jepsen/jepsen/
RUN if [ -f /jepsen/jepsen/project.clj ]; then cd /jepsen/jepsen && lein install; fi
COPY jepsen /jepsen/

ADD --chown=jepsen:jepsen ./bashrc /home/jepsen/.bashrc
ADD --chown=jepsen:jepsen ./init.sh /home/jepsen/init.sh
RUN chmod +x /home/jepsen/init.sh

CMD /home/jepsen/init.sh
