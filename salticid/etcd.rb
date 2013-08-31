role :etcd do
  task :setup do
    sudo do
      # Select your architecture
      gofile = 'go1.1.2.linux-386.tar.gz'
      # gofile = 'go1.1.2.linux-amd64.tar.gz'

      # Go
      cd '/opt'
      unless dir? 'go'
        unless file? gofile
          exec! "wget https://go.googlecode.com/files/#{gofile}", :echo => true
        end
        exec! "tar -xzf #{gofile}"

        exec "ln -sf /opt/go/bin/go /usr/bin/go"
        
        s1 = "export GOROOT=/opt/go"
        s2 = "export PATH=$PATH:$GOROOT/bin"
        exec! "echo '#{s1}' >> /etc/bash.bashrc"
        exec! "echo '#{s2}' >> /etc/bash.bashrc"
      end
    end

    # Re-sudo should set the ENV vars for go
    sudo do
      cd '/opt'
      unless dir? 'etcd'
        git :clone, 'https://github.com/coreos/etcd.git', :echo => true
      end
      cd :etcd

      exec! "GOROOT=/opt/go ./build", :echo => true
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
      exec! 'ps a | grep etcd | grep -v grep | awk \'{ print $1 }\' | xargs kill -s kill'
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
