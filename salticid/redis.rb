require 'fileutils'

role :redis do
  task :setup do
    sudo do
      cd '/opt/'
      unless dir? :redis
        git :clone, 'git://github.com/antirez/redis.git', echo: true
      end
      cd :redis
      git :checkout, :unstable
      make :clean, echo: true
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
      echo "port 26379
sentinel monitor mymaster #{dig '+short', name} 6379 3
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
