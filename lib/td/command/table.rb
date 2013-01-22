
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
    force = false
    op.on('-f', '--force', 'never prompt', TrueClass) {|b|
      force = true
    }

    db_name, table_name = op.cmd_parse

    client = get_client

    begin
      unless force
        table = get_table(client, db_name, table_name)
        $stderr.print "Do you really delete '#{table_name}' in '#{db_name}'? [y/N]: "
        ok = nil
        while line = $stdin.gets
          line.strip!
          if line =~ /^y(?:es)?$/i
            ok = true
            break
          elsif line.empty? || line =~ /^n(?:o)?$/i
            break
          else
            $stderr.print "Type 'Y' or 'N': "
          end
        end
        unless ok
          $stderr.puts "canceled."
          exit 1
        end
      end
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
    require 'parallel'

    num_threads = 4
    op.on('-n', '--num_threads VAL', 'number of threads to get list in parallel') { |i|
      num_threads = Integer(i)
    }

    db_name = op.cmd_parse

    client = get_client

    if db_name
      database = get_database(client, db_name)
      databases = [database]
    else
      databases = client.databases
    end

    rows = []
    ::Parallel.each(databases, :in_threads => num_threads) {|db|
      db.tables.each {|table|
        pschema = table.schema.fields.map {|f|
          "#{f.name}:#{f.type}"
        }.join(', ')
        rows << {:Database => db.name, :Table => table.name, :Type => table.type.to_s, :Count => table.count.to_s,
          :Size => table.estimated_storage_size_string, 'Last import' => table.last_import ? table.last_import.localtime : nil, :Schema => pschema}
      }
    }
    rows = rows.sort_by {|map|
      [map[:Database], map[:Type].size, map[:Table]]
    }

    puts cmd_render_table(rows, :fields => [:Database, :Table, :Type, :Count, :Size, 'Last import', :Schema], :max_width=>500)

    if rows.empty?
      if db_name
        $stderr.puts "Database '#{db_name}' has no tables."
        $stderr.puts "Use '#{$prog} table:create <db> <table>' to create a table."
      elsif dbs.empty?
        $stderr.puts "There are no databases."
        $stderr.puts "Use '#{$prog} db:create <db>' to create a database."
      else
        $stderr.puts "There are no tables."
        $stderr.puts "Use '#{$prog} table:create <db> <table>' to create a table."
      end
    end
  end

  def table_swap(op)
    db_name, table_name1, table_name2 = op.cmd_parse

    client = get_client

    table1 = get_table(client, db_name, table_name1)
    table2 = get_table(client, db_name, table_name2)

    client.swap_table(db_name, table_name1, table_name2)

    $stderr.puts "'#{db_name}.#{table_name1}' and '#{db_name}.#{table_name2}' are swapped."
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
    count = nil
    pretty = nil

    op.on('-t', '--to TIME', 'end time of logs to get') {|s|
      if s.to_i.to_s == s
        to = s.to_i
      else
        require 'time'
        to = Time.parse(s).to_i
      end
    }
    op.on('-f', '--from TIME', 'start time of logs to get') {|s|
      if s.to_i.to_s == s
        from = s.to_i
      else
        require 'time'
        from = Time.parse(s).to_i
      end
    }
    op.on('-n', '--count N', 'number of logs to get', Integer) {|i|
      count = i
    }
    op.on('-P', '--pretty', 'pretty print', TrueClass) {|b|
      pretty = b
    }

    if count == nil
      # smart count calculation
      begin
        require "curses"
	      if Curses.stdscr.maxy - 1 <= 40
          count = 5
        else
          count = 10
        end
	      Curses.close_screen
      rescue Exception
        count = 5
      end
    end

    db_name, table_name = op.cmd_parse

    client = get_client

    table = get_table(client, db_name, table_name)

    rows = table.tail(count, to, from)

    require 'json'
    if pretty
      opts = {
        :indent => ' '*2,
        :object_nl => "\n",
        :space => ' '
      }
      rows.each {|row|
        puts row.to_json(opts)
      }
    else
      rows.each {|row|
        puts row.to_json
      }
    end
  end

  def table_export(op)
    from = nil
    to = nil
    s3_bucket = nil
    wait = false

    ## TODO
    #op.on('-t', '--to TIME', 'end time of logs to get') {|s|
    #  if s.to_i.to_s == s
    #    to = s
    #  else
    #    require 'time'
    #    to = Time.parse(s).to_i
    #  end
    #}
    #op.on('-f', '--from TIME', 'start time of logs to get') {|s|
    #  if s.to_i.to_s == s
    #    from = s
    #  else
    #    require 'time'
    #    from = Time.parse(s).to_i
    #  end
    #}
    op.on('-w', '--wait', 'wait for finishing the job', TrueClass) {|b|
      wait = b
    }
    op.on('--s3-bucket NAME', 'name of the s3 bucket to output') {|s|
      s3_bucket = s
    }

    db_name, table_name = op.cmd_parse

    unless s3_bucket
      $stderr.puts "--s3-bucket NAME option is required"
      exit 1
    end

    client = get_client

    table = get_table(client, db_name, table_name)

    opts = {}
    opts['s3_bucket'] = s3_bucket
    opts['s3_file_format'] ='json.gz'
    opts['from'] = from.to_s if from
    opts['to']   = to.to_s if to

    job = table.export('s3', opts)

    $stderr.puts "Export job #{job.job_id} is queued."
    $stderr.puts "Use '#{$prog} job:show #{job.job_id}' to show the status."

    if wait && !job.finished?
      wait_job(job)
      puts "Status     : #{job.status}"
    end
  end

  def table_partial_delete(op)
    org = nil
    from = nil
    to = nil
    wait = false

    op.on('-g', '--org ORGANIZATION', "delete data partially under this organization") {|s|
      org = s
    }
    op.on('-t', '--to TIME', 'end time of logs to delete') {|s|
      if s.to_i.to_s == s
        # UNIX time
        to = s.to_i
      else
        require 'time'
        to = Time.parse(s).to_i
      end
    }
    op.on('-f', '--from TIME', 'start time of logs to delete') {|s|
      if s.to_i.to_s == s
        from = s.to_i
      else
        require 'time'
        from = Time.parse(s).to_i
      end
    }
    op.on('-w', '--wait', 'wait for finishing the job', TrueClass) {|b|
      wait = b
    }

    db_name, table_name = op.cmd_parse

    unless from
      $stderr.puts "-f, --from TIME option is required"
      exit 1
    end

    unless to
      $stderr.puts "-t, --to TIME option is required"
      exit 1
    end

    if from % 3600 != 0 || to % 3600 != 0
      $stderr.puts "time must be a multiple of 3600 (1 hour)"
      exit 1
    end

    client = get_client

    table = get_table(client, db_name, table_name)

    opts = {}
    opts['organization'] = org if org
    job = client.partial_delete(db_name, table_name, to, from, opts)

    $stderr.puts "Partial delete job #{job.job_id} is queued."
    $stderr.puts "Use '#{$prog} job:show #{job.job_id}' to show the status."

    if wait && !job.finished?
      wait_job(job)
      puts "Status     : #{job.status}"
    end
  end

  require 'td/command/import'  # table:import
  require 'td/command/export'  # table:export
  require 'td/command/job'  # wait_job
end
end

