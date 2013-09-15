require 'fileutils'

load __DIR__/'etcd.rb'
load __DIR__/'mongo.rb'
load __DIR__/'nuodb.rb'
load __DIR__/'postgres.rb'
load __DIR__/'redis.rb'
load __DIR__/'riak.rb'

role :base do
  task :setup do
    sudo do
      exec! 'apt-get install -y ntp curl wget build-essential git-core vim psmisc iptables dnsutils telnet nmap', echo: true
    end
  end

  task :shutdown do
    sudo { shutdown '-h', :now }
  end

  task :reboot do
    sudo { reboot }
  end
end

role :jepsen do
  task :setup do
    base.setup
    etcd.setup
    mongo.setup
    postgres.setup
    redis.setup
    riak.setup
  end
 
  task :slow do
    sudo { exec! 'tc qdisc add dev eth0 root netem delay 50ms 10ms distribution normal' }
  end

  task :flaky do
    sudo { exec! 'tc qdisc add dev eth0 root netem loss 20% 75%' }
  end

  task :fast do
    sudo { tc :qdisc, :del, :dev, :eth0, :root }
  end

  task :partition do
    sudo do
      n3 = (getent 'ahosts', :n3).split("\n").reject {|line| line.start_with?("127.")}.first.split(" ").first
      n4 = (getent 'ahosts', :n4).split("\n").reject {|line| line.start_with?("127.")}.first.split(" ").first
      n5 = (getent 'ahosts', :n5).split("\n").reject {|line| line.start_with?("127.")}.first.split(" ").first
      if ['n1', 'n2'].include? name
        log "Partitioning from n3, n4 and n5."
        iptables '-A', 'INPUT', '-s', n3, '-j', 'DROP'
        iptables '-A', 'INPUT', '-s', n4, '-j', 'DROP'
        iptables '-A', 'INPUT', '-s', n5, '-j', 'DROP'
      end
      iptables '--list', echo: true
    end
  end

  task :partition_reject do
    sudo do
      n1 = (getent 'ahosts', :n1).split("\n").reject {|line| line.start_with?("127.")}.first.split(" ").first
      n2 = (getent 'ahosts', :n2).split("\n").reject {|line| line.start_with?("127.")}.first.split(" ").first
      n3 = (getent 'ahosts', :n3).split("\n").reject {|line| line.start_with?("127.")}.first.split(" ").first
      n4 = (getent 'ahosts', :n4).split("\n").reject {|line| line.start_with?("127.")}.first.split(" ").first
      n5 = (getent 'ahosts', :n5).split("\n").reject {|line| line.start_with?("127.")}.first.split(" ").first
      if ['n1', 'n2'].include? name
        log "Partitioning from n3, n4 and n5."
        iptables '-A', 'INPUT', '-s', n3, '-j', 'REJECT'
        iptables '-A', 'INPUT', '-s', n4, '-j', 'REJECT'
        iptables '-A', 'INPUT', '-s', n5, '-j', 'REJECT'
      else
        log "Partitioning from n1, n2"
        iptables '-A', 'INPUT', '-s', n1, '-j', 'REJECT'
        iptables '-A', 'INPUT', '-s', n2, '-j', 'REJECT'
      end

      iptables '--list', echo: true
    end
  end

  task :drop_pg do
    sudo do
      log "Dropping all PG traffic."
      iptables '-A', 'INPUT', '-p', 'tcp', '--dport', 5432, '-j', 'DROP'
      iptables '--list', echo: true
    end
  end

  task :heal do
    sudo do
      iptables '-F', echo: true
      iptables '-X', echo: true
      iptables '--list', echo: true
    end
  end

  task :status do
    sudo do
      iptables '--list', echo: true
    end
  end
end

group :jepsen do
  host :n1
  host :n2
  host :n3
  host :n4
  host :n5
  
  each_host do
    user :ubuntu
    role :base
    role :cassandra
    role :etcd
    role :kafka
    role :mongo
    role :nuodb
    role :postgres
    role :redis
    role :riak
    role :zk
    role :jepsen
    @password = 'ubuntu'
  end
end
