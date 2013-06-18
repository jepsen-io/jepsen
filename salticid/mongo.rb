require 'fileutils'

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

  task :rs_stat do
    mongo.eval 'rs.status().members.map(function(m) { print(m.name + " " + m.stateStr + "\t" + m.optime.t + "/" + m.optime.i); }); true'
  end

  task :deploy do
    sudo do
      echo File.read(__DIR__/:mongo/'mongodb.conf').gsub('%%NODE%%', name), to: '/etc/mongodb.conf'
    end
    mongo.eval 'c = rs.conf(); c.members[0].priority = 2; rs.reconfig(c);'
    mongo.restart
  end

  task :flip do
    if name != "n1"
      mongo.eval 'rs.stepDown(30)'
    end
  end

  task :reset do
    sudo do
      if dir? '/var/lib/mongdb/rollback'
        find '/var/lib/mongodb/rollback/', '-iname', '*.bson', '-delete'
      end
      find '/var/log/mongodb/', '-iname', '*.log', '-delete'
      mongo.restart
    end
  end

  # Grabs logfiles and data files and tars them up
  task :collect do
    d = 'mongo-collect/' + name
    FileUtils.mkdir_p d

    # Logs
    download '/var/log/mongodb/mongodb.log', d

    # Oplogs
    #oplogs = d/:oplogs
    #FileUtils.mkdir_p oplogs
    #cd '/tmp'
    #rm '-rf', 'mongo-collect'
    #mkdir 'mongo-collect'
    #mongodump '-d', 'local', '-c', 'oplog.rs', '-o', 'mongo-collect', echo: true
    #cd 'mongo-collect/local'
    #find('*.bson').split("\n").each do |f|
    #  log oplogs
    #  download f, oplogs
    #end
    #cd '/tmp'
    #rm '-rf', 'mongo-collect'

    # Data dirs
    rb = '/var/lib/mongodb/rollback'
    if dir? rb
      FileUtils.mkdir_p "#{d}/rollback"
      find(rb, '-iname', '*.bson').split("\n").each do |f|
        download f, "#{d}/rollback"
      end
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
