role :etcd do
  task :setup do
    sudo do
      etcd_binary = "https://github.com/coreos/etcd/releases/download/v0.2.0-rc2/etcd-v0.2.0-rc2-Linux-x86_64.tar.gz"
      cd '/opt'
      unless dir? 'etcd'
        mkdir :etcd
      end
      cd :etcd

      exec! "wget #{etcd_binary}", :echo => true
    end
  end

  task :nuke do
    etcd.stop
    sudo do
      exec! 'rm -rf /opt/etcd/node-data'
    end
  end

  task :stop do
    sudo do
      exec! 'ps ax | grep etcd | grep -v grep | awk \'{ print $1 }\' | xargs kill -s kill'
    end
  end

  task :start do
    sudo do
      cd '/opt/etcd'
      if name == 'n1'
        exec! "./etcd -s 0.0.0.0:7001 -c 0.0.0.0:4001 -d node-data -n #{name}", :echo => true
      else
        exec! "./etcd -s 0.0.0.0:7001 -c 0.0.0.0:4001 -d node-data -n #{name} -C n1:7001", :echo => true
      end
      
    end
  end
  
  task :restart do
    etcd.stop
    etcd.start
  end
end
