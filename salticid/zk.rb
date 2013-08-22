role :zk do
  task :setup do
    sudo do
      exec! 'apt-get install -y zookeeper zookeeper-bin zookeeperd'
    end
    zk.deploy
  end

  task :tail do
    tail '-F', '/var/log/zookeeper/zookeeper.log', echo: true
  end

  task :start do
    sudo do
      service :zookeeper, :start
    end
  end

  task :stop do
    sudo do
      service :zookeeper, :stop
    end
  end

  task :restart do
    sudo do
      service :zookeeper, :restart
    end
  end

  task :nuke do
    sudo do
      zk.stop rescue nil
      exec! 'rm -rf /var/lib/zookeeper/version-*'
      exec! 'rm -rf /var/log/zookeeper/*'
    end
  end

  task :deploy do
    sudo do
      # We name our nodes in reverse order so the leader is n1.
      name =~ /(\d+)/
      echo (6 - $1.to_i), to: "/etc/zookeeper/conf/myid"
      sudo_upload __DIR__/:zk/'zoo.cfg', '/etc/zookeeper/conf/zoo.cfg'
    end
    zk.restart
  end
end
