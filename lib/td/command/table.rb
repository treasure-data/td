
module TreasureData
module Command

  def table_create(op)
    db_name, table_name = op.cmd_parse

    #API.validate_database_name(db_name)
    API.validate_table_name(table_name)

    client = get_client

    begin
      client.create_log_table(db_name, table_name)
    rescue NotFoundError
      cmd_debug_error $!
      $stderr.puts "Database '#{db_name}' does not exist."
      $stderr.puts "Use '#{$prog} db:create #{db_name}' to create the database."
      exit 1
    rescue AlreadyExistsError
      cmd_debug_error $!
      $stderr.puts "Table '#{db_name}.#{table_name}' already exists."
      exit 1
    end

    $stderr.puts "Table '#{db_name}.#{table_name}' is created."
  end

  def table_delete(op)
    db_name, table_name = op.cmd_parse

    client = get_client

    begin
      client.delete_table(db_name, table_name)
    rescue NotFoundError
      cmd_debug_error $!
      $stderr.puts "Table '#{db_name}.#{table_name}' does not exist."
      $stderr.puts "Use '#{$prog} table:list #{db_name}' to show list of the tables."
      exit 1
    end

    $stderr.puts "Table '#{db_name}.#{table_name}' is deleted."
  end

  def table_list(op)
    db_name = op.cmd_parse

    client = get_client

    if db_name
      db = get_database(client, db_name)
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
        $stderr.puts "Use '#{$prog} table:create <db.table>' to create a table."
      elsif dbs.empty?
        $stderr.puts "There are no databases."
        $stderr.puts "Use '#{$prog} db:create <db>' to create a database."
      else
        $stderr.puts "There are no tables."
        $stderr.puts "Use '#{$prog} table:create <db.table>' to create a table."
      end
    end
  end

  def table_show(op)
    db_name, table_name = op.cmd_parse

    client = get_client

    table = get_table(client, db_name, table_name)

    puts "Name      : #{table.db_name}.#{table.name}"
    puts "Type      : #{table.type}"
    puts "Count     : #{table.count}"
    puts "Schema    : ("
    table.schema.fields.each {|f|
      puts "    #{f.name}:#{f.type}"
    }
    puts ")"
  end

  def table_tail(op)
    from = nil
    to = nil
    num = 80

    op.on('-f', '--from TIME', 'start time of logs to get') {|s|
      from = Time.parse(s).to_i
    }
    op.on('-t', '--to TIME', 'end time of logs to get') {|s|
      to = Time.parse(s).to_i
    }
    op.on('-n', '--num N', 'number of logs to get', Integer) {|i|
      num = i
    }

    db_name, table_name = op.cmd_parse

    client = get_client

    table = get_table(client, db_name, table_name)

    rows = table.tail(num, to, from)

    require 'json'
    rows.each {|row|
      puts row.to_json
    }
  end

  require 'td/command/import'  # table:import
end
end

