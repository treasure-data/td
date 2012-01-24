
module TreasureData
module Command

  def schema_show(op)
    db_name, table_name = op.cmd_parse

    client = get_client
    table = get_table(client, db_name, table_name)

    puts "#{db_name}.#{table_name} ("
    table.schema.fields.each {|f|
      puts "  #{f.name}:#{f.type}"
    }
    puts ")"
  end

  def schema_set(op)
    db_name, table_name, *columns = op.cmd_parse
    schema = parse_columns(columns)

    client = get_client
    table = get_table(client, db_name, table_name)

    client.update_schema(table.db_name, table.name, schema)

    $stderr.puts "Schema is updated on #{db_name}.#{table_name} table."
  end

  def schema_add(op)
    db_name, table_name, *columns = op.cmd_parse
    schema = parse_columns(columns)

    client = get_client
    table = get_table(client, db_name, table_name)

    schema = table.schema.merge(schema)

    client.update_schema(table.db_name, table.name, schema)

    $stderr.puts "Schema is updated on #{db_name}.#{table_name} table."
  end

  def schema_remove(op)
    db_name, table_name, *columns = op.cmd_parse

    client = get_client
    table = get_table(client, db_name, table_name)

    schema = table.schema

    columns.each {|col|
      deleted = false
      schema.fields.delete_if {|f|
        f.name == col && deleted = true
      }
      unless deleted
        $stderr.puts "Column name '#{col}' does not exist."
        exit 1
      end
    }

    client.update_schema(table.db_name, table.name, schema)

    $stderr.puts "Schema is updated on #{db_name}.#{table_name} table."
  end

  private
  def parse_columns(columns)
    schema = Schema.new

    columns.each {|column|
      name, type = column.split(':',2)
      name = name.to_s
      type = type.to_s

      API.validate_column_name(name)
      #type = API.normalize_type_name(type)

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

    schema
  end
end
end

