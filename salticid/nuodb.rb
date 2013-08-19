role :nuodb do
  task :tail do
    tail '-F', '/var/log/nuodb/agent.log', echo: true
  end

  task :setup do
    # Get package
    cd '/tmp'
    version = '1.2'
    md5 = '3811457767612140abf0f014c4906e4f'
    file = "nuodb-#{version}.linux.x64.deb"
    unless (md5sum(file) =~ /#{md5}\s+#{file}/ rescue false)
      # Need to re-download file.
      rm file rescue nil
      log "Downloading #{file}"
      wget '-nv', 'http://www.nuodb.com/latest/' + file, echo: true
    end

    sudo do
      # Install
      dpkg '-i', file, echo: true

      # Init script
      cp "/opt/nuodb/etc/nuoagent", "/etc/init.d/"
      chmod 0755, '/etc/init.d/nuoagent'
      exec! 'update-rc.d nuoagent defaults 98 02'

      # Set up data dir
      mkdir '-p', "/opt/nuodb/data"
      chown 'nuodb:nuodb', '/opt/nuodb/data'
    end

    nuodb.deploy
    
    # License file
    license = '/opt/nuodb/etc/license'
    sudo_upload __DIR__/:nuodb/:license, license
    sudo { chmod 0644, license }
    begin
      unless nuodb.manager("show domain license") =~ /Developer Edition/
        log "Applying new license. This is gonna crash Salticid. :("
        log nuodb.manager "apply domain license licenseFile #{license}"
      end
    rescue => e
    end
  end

  task :stop do
    sudo do
      service :nuoagent, :stop, echo: true
      killall :nuodb rescue nil
    end
  end

  task :start do
    sudo do
      service :nuoagent, :start, echo: true
    end
  end

  task :restart do
    nuodb.stop
    nuodb.start
  end

  task :manager do |line|
    java '-jar', '/opt/nuodb/jar/nuodbmanager.jar',
      '--broker', 'localhost', '--password', 'jepsen',
      '--command', line
  end

  task :deploy do
    # Copy default.properties
    # lmao it's possible to self-join; worst database ever
    peer = dig '+short', (name == "n1" ? "n2" : "n1")
    sudo :nuodb do
      echo File.read(__DIR__/:nuodb/'default.properties').
        gsub('%%PEER%%', peer),
        to: '/opt/nuodb/etc/default.properties'
    end
    sudo_upload __DIR__/:nuodb/'webapp.properties', '/opt/nuodb/etc/webapp.properties'

    nuodb.stop

    # Block until n1 ready
    unless name == "n1"
      @@nuodb_ready ||= false
      ready = false
      until ready do
        salticid.mutex.synchronize do
          sleep 1
          log "waiting for n1"
          ready = @@nuodb_ready
        end
      end
    end

    # Restart manager
    nuodb.start

    # Set up storage manager
    log nuodb.manager "start process sm host #{name} database jepsen archive /opt/nuodb/data initialize true options '--dba-user jepsen --dba-password jepsen'"

    # Set up transaction engine
    log nuodb.manager "start process te host #{name} database jepsen options '--dba-user jepsen --dba-password jepsen'"

    # Let others know they can run
    salticid.mutex.synchronize do
      log 'other nodes may proceed'
      @@nuodb_ready = true
    end
  end
end
