
module TreasureData
module Command

  def db_show(op)
    set_render_format_option(op)

    db_name = op.cmd_parse

    client = get_client

    db = get_database(client, db_name)

    rows = []
    db.tables.each {|table|
      pschema = table.schema.fields.map {|f|
        "#{f.name}:#{f.type}"
      }.join(', ')
      rows << {:Table => table.name, :Type => table.type.to_s, :Count => table.count.to_s, :Schema=>pschema.to_s}
    }
    rows = rows.sort_by {|map|
      [map[:Type].size, map[:Table]]
    }

    $stdout.puts cmd_render_table(rows, :fields => [:Table, :Type, :Count, :Schema], :render_format => op.render_format)
  end

  def db_list(op)
    set_render_format_option(op)

    op.cmd_parse

    client = get_client
    dbs = client.databases

    rows = []
    dbs.each {|db|
      rows << {:Name=>db.name, :Count=>db.count}
    }
    $stdout.puts cmd_render_table(rows, :fields => [:Name, :Count], :render_format => op.render_format)

    if dbs.empty?
      $stderr.puts "There are no databases."
      $stderr.puts "Use '#{$prog} " + Config.cl_options_string + "db:create <db_name>' to create a database."
    end
  end

  def db_create(op)
    db_name = op.cmd_parse

    API.validate_database_name(db_name)

    client = get_client

    opts = {}
    begin
      client.create_database(db_name, opts)
    rescue AlreadyExistsError
      $stderr.puts "Database '#{db_name}' already exists."
      exit 1
    end

    $stderr.puts "Database '#{db_name}' is created."
    $stderr.puts "Use '#{$prog} " + Config.cl_options_string + "table:create #{db_name} <table_name>' to create a table."
  end

  def db_delete(op)
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
      exit 1
    end

    $stderr.puts "Database '#{db_name}' is deleted."
  end

end
end

