role :etcd do
  etcd_download_url = "https://github.com/coreos/etcd/releases/download/v0.2.0-rc2/etcd-v0.2.0-rc2-Linux-x86_64.tar.gz"
  etcd_tarball = File.basename(etcd_download_url)
  etcd_dir = etcd_tarball[0...-7] # assumes it is a .tar.gz, not something like tgz
  task :setup do
    sudo do
      cd '/opt'
      unless dir? 'etcd'
        mkdir :etcd
      end
      cd :etcd

      exec! "wget #{etcd_binary}", :echo => true
      exec! "tar xzf #{etcd_tarball}"
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
      puts "/opt/etcd/#{etcd_dir}"
      cd "/opt/etcd/#{etcd_dir}"
      if name == 'n1'
        exec! "./etcd -peer-addr 0.0.0.0:7001 -addr 0.0.0.0:4001 -data-dir node-data -name #{name} -peer-bind-addr 0.0.0.0 -bind-addr 0.0.0.0", :echo => true
      else
        exec! "./etcd -peer-addr 127.0.0.1:7001 -addr 127.0.0.1:4001 -data-dir node-data -name #{name} -peers n1:7001 -peer-bind-addr 0.0.0.0 -bind-addr 0.0.0.0", :echo => true
      end
    end
  end
  
  task :restart do
    etcd.stop
    etcd.start
  end
end
