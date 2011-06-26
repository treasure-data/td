
module TRD
module Command

  def create_database
    op = cmd_opt 'create-database', :db_name
    db_name = op.cmd_parse

    conf = cmd_config
    api = cmd_api(conf)

    begin
      api.create_database(db_name)
    rescue AlreadyExistsError
      $stderr.puts "Database '#{db_name}' already exists."
      exit 1
    end

    $stderr.puts "Database '#{db_name}' is created."
    $stderr.puts "Use '#{$prog} create-log-table #{db_name} <table_name>' to create a table."
  end

  def drop_database
    op = cmd_opt 'drop-database', :db_name

    op.banner << "\noptions:\n"

    force = false
    op.on('-f', '--force', 'clear tables and delete the database', TrueClass) {|b|
      force = true
    }

    db_name = op.cmd_parse

    conf = cmd_config
    api = cmd_api(conf)

    begin
      db = api.database(db_name)

      if !force && !db.tables.empty?
        $stderr.puts "Database '#{db_name}' is not empty. Use '-f' option or drop tables first."
        exit 1
      end

      db.delete
    rescue NotFoundError
      $stderr.puts "Database '#{db_name}' does not exist."
      eixt 1
    end

    $stderr.puts "Database '#{db_name}' is deleted."
  end

  def show_databases
    op = cmd_opt 'show-databases'
    op.cmd_parse

    conf = cmd_config
    api = cmd_api(conf)

    dbs = api.databases

    rows = []
    dbs.each {|db|
      rows << {:Name => db.name}
    }
    puts cmd_render_table(rows, :fields => [:Name])

    if dbs.empty?
      $stderr.puts "There are no databases."
      $stderr.puts "Use '#{$prog} create-database <db_name>' to create a database."
    end
  end

  alias show_dbs show_databases
  alias create_db create_database
  alias drop_db drop_database

end
end

