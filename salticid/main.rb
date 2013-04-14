role :base do
  task :setup do
    sudo do
      exec! 'apt-get install -y curl wget build-essential git-core vim psmisc iptables dnsutils telnet nmap', echo: true
    end
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
      exec! 'apt-get install -y mongodb-server'
    end
  end
end

role :redis do
  task :setup do
    sudo do
      exec! 'apt-get install -y redis-server'
    end
  end
end

role :postgres do
  task :setup do
    sudo do
      exec! 'apt-get install -y postgresql-9.1'
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
