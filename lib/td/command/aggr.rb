
module TreasureData
module Command

  def aggr_list(op)
    op.cmd_parse

    client = get_client

    ass = client.aggregation_schemas

    rows = []
    ass.each {|as|
      rows << {:Name=>as.name, :Relation=>as.relation_key, :Timezone=>as.timezone}
    }

    puts cmd_render_table(rows, :fields => [:Name, :Relation, :Timezone])

    if rows.empty?
      $stderr.puts "There are no aggregation schemas."
      $stderr.puts "Use '#{$prog} aggr:create <name>' to create a aggregation schema."
    end
  end

  def aggr_show(op)
    name = op.cmd_parse

    client = get_client

    begin
      as = client.aggregation_schema(name)
    rescue
      cmd_debug_error $!
      $stderr.puts "Aggregation '#{name}' does not exist."
      exit 1
    end

    log_rows = []
    as.logs.each {|las|
      log_rows << {
        :Table=>las.table.identifier,
        :Name=>las.name,
        :o1_key=>las.okeys[0].to_s,
        :o2_key=>las.okeys[1].to_s,
        :o3_key=>las.okeys[2].to_s,
        :value_key=>las.value_key.to_s,
        :count_key=>las.count_key.to_s,
        :Comment=>las.comment.to_s
      }
    }

    attr_rows = []
    as.attributes.each {|aas|
      params = aas.parameters.to_a.map {|k,v| "#{k}=#{v}" }.join(' ')
      attr_rows << {
        :Table=>aas.table.identifier,
        :Name=>aas.name,
        :Method=>aas.method_name,
        :Parameters=>params,
        :Comment=>aas.comment.to_s,
      }
    }

    puts "Log entries:"
    puts cmd_render_table(log_rows, :fields => [:Table, :Name, :o1_key, :o2_key, :o3_key, :value_key, :count_key, :Comment], :max_width=>400)

    puts ''

    puts "Attribute entries:"
    puts cmd_render_table(attr_rows, :fields => [:Table, :Name, :Method, :Parameters, :Comment], :max_width=>400)
  end

  def aggr_create(op)
    timezone = nil

    op.on('-t', '--timezone TZ', 'name of the timezone (like Asia/Tokyo)') {|s|
      timezone = s
    }

    name, relation_key = op.cmd_parse

    client = get_client

    begin
      client.create_aggregation_schema(name, relation_key, {'timezone'=>timezone})
    rescue AlreadyExistsError
      cmd_debug_error $!
      $stderr.puts "Aggregation '#{name}' already exists."
      exit 1
    end

    $stderr.puts "Aggregation schema '#{name}' is created."
  end

  def aggr_delete(op)
    name = op.cmd_parse

    client = get_client

    client.delete_aggregation_schema(name)

    $stderr.puts "Aggregation schema '#{name}' is deleted."
  end

  def aggr_add_log(op)
    comment = nil
    value_key = nil
    count_key = nil

    op.on('-m', '--comment COMMENT', 'comment of this entry') {|s|
      comment = s
    }
    op.on('-v', '--value KEY_NAME', 'key name of value field') {|s|
      value_key = s
    }
    op.on('-c', '--count KEY_NAME', 'key name of count field') {|s|
      count_key = s
    }

    name, db_name, table_name, entry_name, o1_key, o2_key, o3_key = op.cmd_parse

    okeys = [o1_key, o2_key, o3_key].compact

    client = get_client

    get_table(client, db_name, table_name)

    begin
      client.create_aggregation_log_entry(name, entry_name, comment, db_name, table_name, okeys, value_key, count_key)
    rescue NotFoundError
      cmd_debug_error $!
      $stderr.puts "Aggregation schema '#{name}' does not exist."
      $stderr.puts "Use '#{$prog} aggr:create #{name}' to create the aggregation schema."
      exit 1
    rescue AlreadyExistsError
      $stderr.puts "Aggregation log entry '#{entry_name}' already exists."
      exit 1
    end

    $stderr.puts "Aggregation log entry '#{entry_name}' is created."
  end

  def aggr_add_attr(op)
    comment = nil

    op.on('-m', '--comment COMMENT', 'comment of this entry') {|s|
      comment = s
    }

    name, db_name, table_name, entry_name, method_name, *parameters = op.cmd_parse

    params = {}
    parameters.each {|pa|
      k, v = pa.split('=')
      params[k] = v
    }

    client = get_client

    get_table(client, db_name, table_name)

    begin
      client.create_aggregation_attr_entry(name, entry_name, comment, db_name, table_name, method_name, params)
    rescue NotFoundError
      cmd_debug_error $!
      $stderr.puts "Aggregation schema '#{name}' does not exist."
      $stderr.puts "Use '#{$prog} aggr:create #{name}' to create the aggregation schema."
      exit 1
    rescue AlreadyExistsError
      $stderr.puts "Aggregation attribute entry '#{entry_name}' already exists."
      exit 1
    end

    $stderr.puts "Aggregation attribute entry '#{entry_name}' is created."
  end

  def aggr_del_log(op)
    name, entry_name = op.cmd_parse

    client = get_client

    begin
      client.delete_aggregation_log_entry(name, entry_name)
    rescue NotFoundError
      $stderr.puts "Aggregation log entry '#{entry_name}' does not exist."
      exit 1
    end

    $stderr.puts "Aggregation log entry '#{entry_name}' is deleted."
  end

  def aggr_del_attr(op)
    name, entry_name = op.cmd_parse

    client = get_client

    begin
      client.delete_aggregation_attr_entry(name, entry_name)
    rescue NotFoundError
      $stderr.puts "Aggregation log entry '#{entry_name}' does not exist."
      exit 1
    end

    $stderr.puts "Aggregation log entry '#{entry_name}' is deleted."
  end

end
end
