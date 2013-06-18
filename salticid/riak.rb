require 'fileutils'

role :riak do
  task :setup do
    sudo do
      exec! 'apt-get install -y libssl0.9.8 erlang', echo: true
      cd '/opt'
      unless dir? 'riak'
        git :clone, 'git://github.com/basho/riak.git', echo: true
      end
      cd 'riak'
      begin
        git :checkout, '1.3'
      rescue
        git :checkout, '-b', '1.3', 'origin/1.3'
      end
      git :pull, echo: true
      make :distclean, echo: true
      make :rel, echo: true
    end

    riak.deploy

  end

  task :start do
    sudo do
      cd '/opt/riak/rel/riak'
      exec! 'bash -c "ulimit -n 10000 && bin/riak start"', echo: true
    end
  end
  
  task :restart do
    sudo do
      riak.stop rescue false
      riak.start
    end
  end

  task :stop do
    sudo do
      begin
        cd '/opt/riak/rel/riak'
        exec! 'bin/riak stop', echo: true
      rescue
        killall '-9', 'beam.smp'
      end
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
    riak.stop rescue nil
    sudo do
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

  task :transfers do
    cd '/opt/riak/rel/riak'
    sudo do
      exec! 'bin/riak-admin transfers', echo: true
    end
  end
  
  task :status do
    cd '/opt/riak/rel/riak'
    sudo do
      exec! 'bin/riak-admin status', echo: true
    end
  end

  task :reset do
    sudo do
      riak.stop rescue false
      rm '-rf', '/opt/riak/rel/riak/data/*'
    end
  end

  task :nuke do
    sudo do
      begin
        riak.stop
      rescue
      end
      rm '-rf', '/opt/riak'
    end
  end
end
