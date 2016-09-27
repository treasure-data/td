module TreasureData::Command

  def schema_show(op)
    db_name, table_name = op.cmd_parse

    client = get_client
    table = get_table(client, db_name, table_name)

    $stdout.puts "#{db_name}.#{table_name} ("
    table.schema.fields.each {|f|
      if f.sql_alias
        $stdout.puts "  #{f.name}:#{f.type}@#{f.sql_alias}"
      else
        $stdout.puts "  #{f.name}:#{f.type}"
      end
    }
    $stdout.puts ")"
  end

  def schema_set(op)
    db_name, table_name, *columns = op.cmd_parse

    client = get_client
    table = get_table(client, db_name, table_name)

    schema = TreasureData::Schema.parse(columns)
    client.update_schema(db_name, table_name, schema)

    $stderr.puts "Schema is updated on #{db_name}.#{table_name} table."
  end

  def schema_add(op)
    db_name, table_name, *columns = op.cmd_parse

    client = get_client
    table = get_table(client, db_name, table_name)

    schema = table.schema.merge(TreasureData::Schema.parse(columns))
    client.update_schema(db_name, table_name, schema)

    $stderr.puts "Schema is updated on #{db_name}.#{table_name} table."
  end

  def schema_remove(op)
    db_name, table_name, *columns = op.cmd_parse

    client = get_client
    table = get_table(client, db_name, table_name)

    schema = table.schema

    columns.each {|col|
      unless schema.fields.reject!{|f| f.name == col }
        $stderr.puts "Column name '#{col}' does not exist."
        exit 1
      end
    }

    client.update_schema(db_name, table_name, schema)

    $stderr.puts "Schema is updated on #{db_name}.#{table_name} table."
  end
end
