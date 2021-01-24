# Develop Environment Setup

This project is built base on [Clojure](https://clojure.org/), a modern dialet of Lisp. You can go to the official website to learn more about it, and this [cheatsheet](https://clojure.org/api/cheatsheet) is a good place for you to look up for the semantics of some specific syntax.

In brief, clojure code will first be compiled into java code, and then compiled into java byte code, run by JVM. So of course java is necessary here, to install java:
```
sudo apt install openjdk-11-jdk openjdk-11-jdk-headless libjna-java

# the following 2 commands solves the bug of
# trustAnchors parameter must be non-empty bug
/usr/bin/printf '\xfe\xed\xfe\xed\x00\x00\x00\x02\x00\x00\x00\x00\xe2\x68\x6e\x45\xfb\x43\xdf\xa4\xd9\x92\xdd\x41\xce\xb6\xb2\x1c\x63\x30\xd7\x92' > /etc/ssl/certs/java/cacerts
/var/lib/dpkg/info/ca-certificates-java.postinst configure
```
Next, this project is managed by [leiningen](https://leiningen.org/), which can do package management like maven. You can go to the official website to learn about how to get leiningen. When running a project, lein will first read the project.clj file in your project root, collect the required package from maven repo or the repo you specify, so you dont have to worry about compactability.

Before we step into the actual tutorial, you need one more thing(very important), which is the IDE! (yes you get it!). For me I use [Intellij](https://www.jetbrains.com/idea/) and [Cursive](https://cursive-ide.com/) plugin to write the clojure code. You can first install Intellij and then install Cursive in Intellij plugin.

Now everything is good, we can now dive into our favorite coding (finally~)