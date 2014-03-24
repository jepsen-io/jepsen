require 'fileutils'

role :foundationdb do
  clientsfile = 'foundationdb-clients_2.0.0-1_amd64.deb'
  serverfile = 'foundationdb-server_2.0.0-1_amd64.deb'
  wget_dir = 'https://foundationdb.com/downloads/I_accept_the_FoundationDB_Community_License_Agreement/2.0.0'

  task :setup do
    sudo do
      exec! 'apt-get install -y wget' unless (dpkg '-l').include? 'wget'

      unless (dpkg '-l').include? 'foundationdb-clients'
        exec! "wget #{wget_dir}/#{clientsfile}", :echo => true unless file? clientsfile
        exec! "dpkg -i #{clientsfile}"
      end

      unless (dpkg '-l').include? 'foundationdb-server'
        exec! "wget #{wget_dir}/#{serverfile}", :echo => true unless file? serverfile
        exec! "dpkg -i #{serverfile}"
      end

      log 'Setting up cluster file.'
      if name == 'n1'
        log 'Creating cluster file.'
        exec! "(echo -n 'local:mlCmooR1@' ;  ifconfig eth0 | grep 'inet addr' | awk -F: '{print $2}' | awk '{printf $1}' ; echo -n ':4500') > /etc/foundationdb/fdb.cluster"

        log 'Setting up public ip address.'
        exec! "fdb_public_ip=$(ifconfig eth0 | grep 'inet addr' | awk -F: '{print $2}' | awk '{printf $1}') ; sudo sed -i " + '"s/auto/$fdb_public_ip/" /etc/foundationdb/foundationdb.conf'
        exec! 'sed -i "s/# class =/class = transaction/" /etc/foundationdb/foundationdb.conf'
        exec! "service foundationdb restart"
        
        log 'Configure for single duplication. '
        exec! "fdbcli --exec 'configure new single memory'"

        all_ready = false
        while not all_ready
          status = exec! "fdbcli --exec status"
          log status
          all_ready = true if status =~ /Machines               - 5/
          sleep 1
        end

        log 'Configure for triple duplication. '
        exec! "fdbcli --exec 'configure triple'"

        log 'Updating coordinators.'
        exec! "fdbcli --exec 'coordinators auto'"
      else
        n1 = (getent 'ahosts', :n1).split("\n").reject {|line| line.start_with?("127.")}.first.split(" ").first
        exec! "echo -n 'local:mlCmooR1@#{n1}:4500' > /etc/foundationdb/fdb.cluster"

        log 'Restarting service.'
        exec! "service foundationdb restart"
      end
    end
  end

  task :stop do
    sudo { service :foundationdb, :stop, echo: true }
  end

  task :start do
    sudo { service :foundationdb, :start, echo: true }
  end

  task :restart do
    sudo { service :foundationdb, :restart, echo: true }
  end

  task :clear_logs do
    sudo do
      exec! 'service foundationdb stop'
      exec! 'rm /var/log/foundationdb/*'
      exec! 'service foundationdb start'
    end
  end

  task :collect_logs do
    d = 'fdb-logs/' + name
    FileUtils.mkdir_p d

    exec! 'rm -f -r fdb-logs'
    exec! 'mkdir fdb-logs'

    sudo do
      exec! 'cp /var/log/foundationdb/* fdb-logs'
      download '/home/ubuntu/fdb-logs/', d, :recursive => true
    end
  end

  task :nuke do
    sudo do
      exec! "dpkg -P foundationdb-server foundationdb-clients"
      exec! "userdel foundationdb"
      exec! "rm -rf /etc/foundationdb /var/lib/foundationdb /var/log/foundationdb"
    end
  end
end
