role :base do
  task :setup do
    sudo do
      exec! 'apt-get install -y curl wget build-essential git-core vim psmisc iptables dnsutils telnet nmap', echo: true
    end
  end

  task :shutdown do
    sudo { shutdown '-h', :now }
  end

  task :reboot do
    sudo { reboot }
  end
end

role :riak do
  task :setup do
    sudo do
      exec! 'apt-get install -y libssl0.9.8 erlang', echo: true
      cd '/opt'
      unless dir? 'riak'
        git :clone, 'git://github.com/basho/riak.git', echo: true
      end
      cd 'riak'
      make :rel, echo: true
    end
  end

  task :start do
    sudo do
      cd '/opt/riak/rel/riak'
      exec! 'bash -c "ulimit -n 10000 && bin/riak start"', echo: true
    end
  end
  
  task :restart do
    sudo do
      cd '/opt/riak/rel/riak'
      exec! 'bin/riak start', echo: true
    end
  end

  task :stop do
    sudo do
      cd '/opt/riak/rel/riak'
      exec! 'bin/riak stop', echo: true
    end
  end

  task :ping do
    sudo do
      exec! '/opt/riak/rel/riak/bin/riak ping', echo: true
    end
  end

  task :tail do
    tail '-F', '/opt/riak/rel/riak/log/console.log', echo: true
  end

  task :deploy do
    sudo do
      riak.stop
      echo File.read(__DIR__/:riak/'app.config'), to: '/opt/riak/rel/riak/etc/app.config'
      echo File.read(__DIR__/:riak/'vm.args').gsub('%%NODE%%', name), to: '/opt/riak/rel/riak/etc/vm.args'
    end
    riak.start
  end

  task :join do
    cd '/opt/riak/rel/riak'
    sudo do
      exec! 'bin/riak-admin cluster join riak@n1', echo: true
    end
  end

  task :plan do
    cd '/opt/riak/rel/riak'
    sudo do
      exec! 'bin/riak-admin cluster plan', echo: true
    end
  end

  task :commit do
    cd '/opt/riak/rel/riak'
    sudo do
      exec! 'bin/riak-admin cluster commit', echo: true
    end
  end
  
  task :ring_status do
    cd '/opt/riak/rel/riak'
    sudo do
      exec! 'bin/riak-admin ring_status', echo: true
    end
  end
  
  task :status do
    cd '/opt/riak/rel/riak'
    sudo do
      exec! 'bin/riak-admin status', echo: true
    end
  end
end

role :mongo do
  task :setup do
    sudo do
      unless (dpkg '-l').include? 'mongodb-10gen'
        exec! 'apt-key adv --keyserver keyserver.ubuntu.com --recv 7F0CEB10'
        echo 'deb http://downloads-distro.mongodb.org/repo/ubuntu-upstart dist 10gen', to: '/etc/apt/sources.list.d/10gen.list'
        exec! 'apt-get update', echo: true
      end
      exec! 'apt-get install -y mongodb-10gen', echo: true
      begin
        mongo.start
      rescue => e
        throw unless e.message =~ /already running/
      end
    end
    
    if name == 'n1'
      log "Waiting for mongo to become available"
      loop do
        begin
          mongo '--eval', true
          break
        rescue
          sleep 1
        end
      end
      log "Initiating replica set."
      mongo.eval 'rs.initiate()'
      log "Waiting for replica set to initialize."
      until (mongo('--eval', 'rs.status().members[0].state') rescue '') =~ /1\Z/
        log mongo('--eval', 'rs.status().members')
        sleep 1
      end
      log "Assigning priority."
      mongo.eval 'c = rs.conf(); c.members[0].priority = 2; rs.reconfig(c)'
      
      log "Adding members to replica set."
      mongo.eval 'rs.add("n2")'
      mongo.eval 'rs.add("n3")'
      mongo.eval 'rs.add("n4")'
      mongo.eval 'rs.add("n5")'
    end
  end

  task :nuke do
    sudo do
      mongo.stop rescue nil
      rm '-rf', '/var/lib/mongodb/*'
    end
  end

  task :stop do
    sudo { service :mongodb, :stop, echo: true }
  end

  task :start do
    sudo { service :mongodb, :start, echo: true }
  end

  task :restart do
    sudo { service :mongodb, :restart, echo: true }
  end

  task :tail do
    tail '-F', '/var/log/mongodb/mongodb.log', echo: true
  end

  task :eval do |str|
    unless (str =~ /;/)
      str = "printjson(#{str})"
    end

    mongo '--eval', str, echo: true
  end

  task :rs_conf do
    mongo.eval 'rs.conf()'
  end

  task :rs_status do
    mongo.eval 'rs.status()'
  end

  task :deploy do
    sudo do
      echo File.read(__DIR__/:mongo/'mongodb.conf').gsub('%%NODE%%', name), to: '/etc/mongodb.conf'
    end
    mongo.eval 'c = rs.conf(); c.members[0].priority = 2; rs.reconfig(c);'
    mongo.restart
  end

  task :reset do
    sudo do
      find '/var/lib/mongodb/rollback/', '-iname', '*.bson', '-delete'
    end
  end

  task :rollbacks do
    if dir? '/var/lib/mongodb/rollback'
      find('/var/lib/mongodb/rollback/',
           '-iname', '*.bson').split("\n").each do |f|
        bsondump f, echo: true
      end
      ls '-lah', '/var/lib/mongodb/rollback', echo: true 
    end
  end
end

role :redis do
  task :setup do
    sudo do
      cd '/opt/'
      unless dir? :redis
        git :clone, 'git://github.com/antirez/redis.git', echo: true
      end
      cd :redis
      make echo: true
#      make :test, echo: true
    end
  end

  task :start do
    cd '/opt/redis/src'
    sudo do
      if name == 'n1'
        # master
        exec! 'bash -c "ulimit -n 10000 && ./redis-server"', echo: true
      else
        exec! 'bash -c "ulimit -n 10000 && ./redis-server --slaveof n1 6379"', echo: true
      end
    end
  end

  task :sentinel do
    sudo do
      myip = dig '+short', name
      echo "port 26379
sentinel monitor mymaster #{myip} 6379 3
sentinel down-after-milliseconds mymaster 5000
sentinel failover-timeout mymaster 900000
sentinel can-failover mymaster yes
sentinel parallel-syncs mymaster 5", to: '/opt/redis/sentinel.config'
      cd '/opt/redis/src'
      exec! './redis-sentinel /opt/redis/sentinel.config', echo: true
    end
  end

  task :stop do
    sudo do
      killall 'redis-server' rescue log "no redis-server"
      killall 'redis-sentinel' rescue log "no redis-sentinel"
    end
  end

  task :replication do
    sudo do
      exec! '/opt/redis/src/redis-cli info replication', echo: true
    end
  end
end

role :postgres do
  task :setup do
    sudo do
      exec! 'apt-get install -y postgresql-9.1', echo: true
      exec! 'locale-gen en_US.UTF-8'
      sudo_upload __DIR__/:postgres/'postgresql.conf',
        '/etc/postgresql/9.1/main/postgresql.conf'
      sudo_upload __DIR__/:postgres/'pg_hba.conf',
        '/etc/postgresql/9.1/main/pg_hba.conf'
      service :postgresql, :restart
    end
    sudo :postgres do
      begin
        createuser '--pwprompt', '--no-createdb', '--no-superuser',
          '--no-createrole', 'jepsen', stdin: "jepsen\njepsen\n", echo: true
        createdb '--owner=jepsen', 'jepsen', echo: true
      rescue
      end
    end
  end
end

role :jepsen do
  task :setup do
    base.setup
    riak.setup
    mongo.setup
    redis.setup
    postgres.setup
  end
  
  task :partition do
    sudo do
      n3 = dig '+short', :n3
      n4 = dig '+short', :n4
      n5 = dig '+short', :n5
      if ['n1', 'n2'].include? name
        log "Partitioning from n3, n4 and n5."
        iptables '-A', 'INPUT', '-s', n3, '-j', 'DROP'
        iptables '-A', 'INPUT', '-s', n4, '-j', 'DROP'
        iptables '-A', 'INPUT', '-s', n5, '-j', 'DROP'
      end
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
    role :net
    role :postgres
    role :redis
    role :mongo
    role :riak
    role :jepsen
    @password = 'ubuntu'
  end
end
