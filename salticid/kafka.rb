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
      id = $1

      # Create topic
      exec! '/opt/kafka/bin/kafka-create-topic.sh --partition 5 --replica 5 --topic jepsen --zookeeper localhost:2181'

      echo File.read(__DIR__/:kafka/'server.properties').gsub('%%ID%%', id),
        to: '/opt/kafka/config/server.properties'
    end
    kafka.start
  end

  task :nuke do
    kafka.stop
    sudo do
      rm '-rf', '/tmp/kafka-logs'
    end
  end

  task :stop do
    sudo do
      exec! 'ps aux | grep kafka | grep -v grep | awk \'{ print $2 }\' | xargs kill -s kill'
    end
  end

  task :start do
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
    tail '-F', '/var/log/zookeeper/zookeeper.log', echo: true
  end

  task :status do
    exec! '/opt/kafka/bin/kafka-list-topic.sh --zookeeper localhost:2181',
      echo: true
  end
end
