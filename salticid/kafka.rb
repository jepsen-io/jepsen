role :kafka do
  task :setup do
    sudo do
      exec! 'apt-get update'
      exec! 'apt-get install -y openjdk-7-jdk', echo: true
      cd '/opt'
      unless dir? 'kafka'
        git :clone, 'https://git-wip-us.apache.org/repos/asf/kafka.git'
      end
      cd :kafka
      begin
        git :checkout, '0.8'
      rescue
        git :checkout, '-b', '0.8', 'remotes/origin/0.8'
      end
      exec! "./sbt update", echo: true
      exec! "./sbt package", echo: true
      exec! "./sbt assembly-package-dependency", echo: true
    end
    kafka.deploy
  end

  task :deploy do
    sudo do
      name =~ /(\d+)/
      id = $1.to_i 

      echo File.read(__DIR__/:kafka/'server.properties').gsub('%%ID%%', id.to_s),
        to: '/opt/kafka/config/server.properties'
    end

    # Create topic asynchronously
    Thread.new do
      sleep 20
      log "creating topic"
      exec! '/opt/kafka/bin/kafka-create-topic.sh --partition 5 --replica 5 --topic jepsen --zookeeper localhost:2181'
    end
    kafka.start
  end

  task :nuke do
    zk.nuke
    kafka.stop
    sudo do
      exec! 'rm -rf /opt/kafka/logs/*'
      rm '-rf', '/tmp/kafka-logs'
    end
  end

  task :stop do
    sudo do
      exec! 'ps aux | grep kafka | grep -v grep | awk \'{ print $2 }\' | xargs kill -s kill'
    end
  end

  task :start do
    zk.start rescue true
    name =~ /(\d+)/
    sleep 3 * $1.to_i
    sudo do
      cd '/opt/kafka/'
      exec! 'bin/kafka-server-start.sh config/server.properties', echo: true
    end
  end
  
  task :restart do
    kakfa.stop
    kafka.start
  end

  task :tail do
    tail '-F', '/opt/kafka/logs/server.log', echo: true
  end

  task :status do
    exec! '/opt/kafka/bin/kafka-list-topic.sh --zookeeper localhost:2181',
      echo: true
  end

  task :partition do
    n2 = dig '+short', :n2
    n3 = dig '+short', :n3
    n4 = dig '+short', :n4
    n5 = dig '+short', :n5
    sudo do
      if name == 'n1'
        [n2, n3, n4, n5].each do |ip|
          iptables '-A', 'INPUT',  '-s', ip, '-p', 'tcp', '--dport', 9092, '-j', 'DROP'
        end
      end
      iptables '--list', echo: true
    end
  end
end
