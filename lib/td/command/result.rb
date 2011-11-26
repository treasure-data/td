
module TreasureData
module Command

  def result_info(op)
    op.cmd_parse

    client = get_client

    info = client.result_set_info

    puts "Type       : #{info.type}"
    puts "Host       : #{info.host}"
    puts "Port       : #{info.port}"
    puts "User       : #{info.user}"
    puts "Password   : #{info.password}"
    puts "Database   : #{info.database}"
  end

  def result_list(op)
    op.cmd_parse

    client = get_client

    rsets = client.result_sets

    rows = []
    rsets.each {|rset|
      rows << {:Name => rset.name}
    }
    rows = rows.sort_by {|map|
      map[:Name]
    }

    puts cmd_render_table(rows, :fields => [:Name])

    if rsets.empty?
      $stderr.puts "There are result tables."
      $stderr.puts "Use '#{$prog} result:create <name>' to create a result table."
    end
  end

  def result_create(op)
    name = op.cmd_parse

    API.validate_database_name(name)

    client = get_client

    begin
      client.create_result_set(name)
    rescue AlreadyExistsError
      $stderr.puts "Result table '#{name}' already exists."
      exit 1
    end

    $stderr.puts "Result table '#{name}' is created."
  end

  def result_delete(op)
    name = op.cmd_parse

    client = get_client

    begin
      client.delete_result_set(name)
    rescue NotFoundError
      $stderr.puts "Result table '#{name}' does not exist."
      exit 1
    end

    $stderr.puts "Result table '#{name}' is deleted."
  end

  def result_connect(op)
    mysql = 'mysql'

    op.on('-e', '--execute MYSQL', 'mysql command') {|s|
      mysql = s
    }

    sql = op.cmd_parse

    client = get_client

    info = client.result_set_info

    cmd = [mysql, '-h', info.host, '-P', info.port.to_s, '-u', info.user, "--password=#{info.password}", info.database]

    cmd_start = Time.now

    if sql
      IO.popen(cmd, "w") {|io|
        io.write sql
        io.close
      }
    else
      STDERR.puts "> #{cmd.join(' ')}"
      system(*cmd)
    end

    cmd_alive = Time.now - cmd_start
    if $?.to_i != 0 && cmd_alive < 1.0
      STDERR.puts "Command died within 1 second with exit code #{$?.to_i}."
      STDERR.puts "Please confirm mysql command is installed."
    end
  end

end
end

