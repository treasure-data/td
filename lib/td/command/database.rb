
module TreasureData
module Command

  def database_create
    op = cmd_opt 'database:create', :db_name
    db_name = op.cmd_parse

    client = get_client

    API.validate_database_name(db_name)

    begin
      client.create_database(db_name)
    rescue AlreadyExistsError
      $stderr.puts "Database '#{db_name}' already exists."
      exit 1
    end

    $stderr.puts "Database '#{db_name}' is created."
    $stderr.puts "Use '#{$prog} create-log-table #{db_name} <table_name>' to create a table."
  end

  def database_delete
    op = cmd_opt 'database:delete', :db_name

    op.banner << "\noptions:\n"

    force = false
    op.on('-f', '--force', 'clear tables and delete the database', TrueClass) {|b|
      force = true
    }

    db_name = op.cmd_parse

    client = get_client

    begin
      db = client.database(db_name)

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

  def database_list
    op = cmd_opt 'database:list'
    op.cmd_parse

    client = get_client

    dbs = client.databases

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
end
end

