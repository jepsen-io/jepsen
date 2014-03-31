role :elasticsearch do
  task :setup do
    sudo do
      exec! 'wget https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-1.0.1.deb'
      exec! 'dpkg -i elasticsearch-1.0.1.deb'
    end
  end

  task :start do
    sudo do
      service :elasticsearch, :start
    end
  end

  task :stop do
    sudo do
      service :elasticsearch, :stop
    end
  end

  task :tail do
    sudo do
      tail '-F', '/var/log/elasticsearch/elasticsearch.log', echo: true
    end
  end

end
