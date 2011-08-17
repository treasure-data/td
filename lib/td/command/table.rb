
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

  def create_or_alter_schema_table(mode_create)
    if mode_create
      op = cmd_opt 'create-schema-table', :db_name, :table_name, :schema_name, :columns_
    else
      op = cmd_opt 'alter-schema-table', :db_name, :table_name, :schema_name, :columns_
    end

    db_name, table_name, schema_name, *columns = op.cmd_parse

    schema = Schema.new
    columns.each {|column|
      name, type = column.split(':',2)
      API.validate_column_name(name)
      type = API.normalize_type_name.(type)
      schema.add_field(name, type)
    }

    client = get_client

    find_table(client, db_name, table_name)

    #TODO
    #if mode_create
      client.create_schema_table(db_name, schema_name, table_name, schema)
      $stderr.puts "Schema table #{db_name}.#{schema_name} is created on #{table_name} table."
    #else
    #  client.alter_schema_table(db_name, schema_name, table_name, schema)
    #  $stderr.puts "Schema table #{db_name}.#{schema_name} on #{table_name} table is updated."
    #end
  end
  private :create_or_alter_schema_table

  def create_schema_table
    create_or_alter_schema_table(true)
  end

  #def alter_schema_table
  #  create_or_alter_schema_table(false)
  #end

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
        rows << {:Database => db.name, :Table => table.name, :Type => table.type.to_s, :Count => table.count.to_s}
      }
    }
    rows = rows.sort_by {|map|
      [map[:Database], map[:Type].size, map[:Table]]
    }

    puts cmd_render_table(rows, :fields => [:Database, :Table, :Type, :Count])

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

end
end

