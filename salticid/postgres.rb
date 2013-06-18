require 'fileutils'

role :postgres do
  task :setup do
    sudo do
      exec! 'apt-get install -y postgresql-9.1', echo: true
      exec! 'locale-gen en_US.UTF-8'
      sudo_upload __DIR__/:postgres/'postgresql.conf',
        '/etc/postgresql/9.1/main/postgresql.conf'
      sudo_upload __DIR__/:postgres/'pg_hba.conf',
        '/etc/postgresql/9.1/main/pg_hba.conf'
      service :postgresql, :restart
    end
    sudo :postgres do
      begin
        createuser '--pwprompt', '--no-createdb', '--no-superuser',
          '--no-createrole', 'jepsen', stdin: "jepsen\njepsen\n", echo: true
        createdb '--owner=jepsen', 'jepsen', echo: true
      rescue
      end
    end
  end
end
