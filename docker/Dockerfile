FROM kojiromike/dind
MAINTAINER jake@apache.org

ENV JEPSEN_GIT_URL http://github.com/aphyr/jepsen
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
        wget


RUN cd / && wget https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein && mv /lein /usr/bin 
RUN chmod +x /usr/bin/lein

RUN git clone $JEPSEN_GIT_URL /jepsen
RUN cd /jepsen/jepsen && lein install


ADD ./build-dockerized-jepsen.sh /usr/local/bin/build-dockerized-jepsen.sh
RUN chmod +x /usr/local/bin/build-dockerized-jepsen.sh

ADD ./bashrc /root/.bashrc

