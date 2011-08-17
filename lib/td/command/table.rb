
module TreasureData
module Command

  def create_log_or_item_table(mode_log, db_name, table_name)
    client = get_client

    API.validate_table_name(table_name)

    begin
      if mode_log
        client.create_log_table(db_name, table_name)
      else
        client.create_item_table(db_name, table_name)
      end
    rescue NotFoundError
      cmd_debug_error $!
      $stderr.puts "Database '#{db_name}' does not exist."
      $stderr.puts "Use '#{$prog} create-database #{db_name}' to create the database."
      exit 1
    rescue AlreadyExistsError
      cmd_debug_error $!
      $stderr.puts "Table '#{db_name}.#{table_name}' already exists."
      exit 1
    end

    $stderr.puts "Table '#{db_name}.#{table_name}' is created."
  end
  private :create_log_or_item_table

  def create_log_table
    op = cmd_opt 'create-log-table', :db_name, :table_name
    db_name, table_name = op.cmd_parse

    create_log_or_item_table(true, db_name, table_name)
  end

  def create_item_table
    op = cmd_opt 'create-item-table', :db_name, :table_name
    db_name, table_name = op.cmd_parse

    create_log_or_item_table(false, db_name, table_name)
  end

  def drop_table
    op = cmd_opt 'drop-table', :db_name, :table_name
    db_name, table_name = op.cmd_parse

    client = get_client

    begin
      client.delete_table(db_name, table_name)
    rescue NotFoundError
      cmd_debug_error $!
      $stderr.puts "Table '#{db_name}.#{table_name}' does not exist."
      $stderr.puts "Use '#{$prog} show-tables #{db_name}' to show list of the tables."
      exit 1
    end

    $stderr.puts "Table '#{db_name}.#{table_name}' is deleted."
  end

  def set_schema
    op = cmd_opt 'set-schema', :db_name, :table_name, :columns_?

    db_name, table_name, *columns = op.cmd_parse

    schema = Schema.new
    columns.each {|column|
      name, type = column.split(':',2)
      name = name.to_s
      type = type.to_s

      API.validate_column_name(name)
      type = API.normalize_type_name(type)

      if schema.fields.find {|f| f.name == name }
        $stderr.puts "Column name '#{name}' is duplicated."
        exit 1
      end
      schema.add_field(name, type)

      if name == 'v' || name == 'time'
        $stderr.puts "Column name '#{name}' is reserved."
        exit 1
      end
    }

    client = get_client

    find_table(client, db_name, table_name)

    client.update_schema(db_name, table_name, schema)

    $stderr.puts "Schema is updated on #{db_name}.#{table_name} table."
    $stderr.puts "Use '#{$prog} describe-table #{db_name} #{table_name}' to confirm the schema."
  end

  def show_tables
    op = cmd_opt 'show-tables', :db_name?
    db_name = op.cmd_parse

    client = get_client

    if db_name
      db = find_database(client, db_name)
      dbs = [db]
    else
      dbs = client.databases
    end

    rows = []
    dbs.each {|db|
      db.tables.each {|table|
        pschema = table.schema.fields.map {|f|
          "#{f.name}:#{f.type}"
        }.join(', ')
        rows << {:Database => db.name, :Table => table.name, :Type => table.type.to_s, :Count => table.count.to_s, :Schema=>pschema.to_s}
      }
    }
    rows = rows.sort_by {|map|
      [map[:Database], map[:Type].size, map[:Table]]
    }

    puts cmd_render_table(rows, :fields => [:Database, :Table, :Type, :Count, :Schema])

    if rows.empty?
      if db_name
        $stderr.puts "Database '#{db_name}' has no tables."
        $stderr.puts "Use '#{$prog} create-log-table #{db_name} <table_name>' to create a table."
      elsif dbs.empty?
        $stderr.puts "There are no databases."
        $stderr.puts "Use '#{$prog} create-database <db_name>' to create a database."
      else
        $stderr.puts "There are no tables."
        $stderr.puts "Use '#{$prog} create-log-table <db_name> <table_name>' to create a table."
      end
    end
  end

  def describe_table
    op = cmd_opt 'describe-table', :db_name, :table_name

    db_name, table_name = op.cmd_parse

    client = get_client

    table = find_table(client, db_name, table_name)

    puts "Name      : #{table.db_name}.#{table.name}"
    puts "Type      : #{table.type}"
    puts "Count     : #{table.count}"
    puts "Schema    : ("
    table.schema.fields.each {|f|
      puts "    #{f.name}:#{f.type}"
    }
    puts ")"
  end
end
end

