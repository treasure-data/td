
module TreasureData
module Command

  def query(op)
    org = nil
    db_name = nil
    wait = false
    output = nil
    format = 'tsv'
    render_opts = {}
    result_url = nil
    result_user = nil
    result_ask_password = false
    priority = nil
    retry_limit = nil
    query = nil
    sampling_all = nil
    type = nil

    op.on('-g', '--org ORGANIZATION', "issue the query under this organization") {|s|
      org = s
    }
    op.on('-d', '--database DB_NAME', 'use the database (required)') {|s|
      db_name = s
    }
    op.on('-w', '--wait', 'wait for finishing the job', TrueClass) {|b|
      wait = b
    }
    op.on('-G', '--vertical', 'use vertical table to show results', TrueClass) {|b|
      render_opts[:vertical] = b
    }
    op.on('-o', '--output PATH', 'write result to the file') {|s|
      output = s
    }
    op.on('-f', '--format FORMAT', 'format of the result to write to the file (tsv, csv, json or msgpack)') {|s|
      unless ['tsv', 'csv', 'json', 'msgpack'].include?(s)
        raise "Unknown format #{s.dump}. Supported format: tsv, csv, json, msgpack"
      end
      format = s
    }
    op.on('-r', '--result RESULT_URL', 'write result to the URL (see also result:create subcommand)') {|s|
      result_url = s
    }
    op.on('-u', '--user NAME', 'set user name for the result URL') {|s|
      result_user = s
    }
    op.on('-p', '--password', 'ask password for the result URL') {|s|
      result_ask_password = true
    }
    op.on('-P', '--priority PRIORITY', 'set priority') {|s|
      priority = job_priority_id_of(s)
      unless priority
        raise "unknown priority #{s.inspect} should be -2 (very-low), -1 (low), 0 (normal), 1 (high) or 2 (very-high)"
      end
    }
    op.on('-R', '--retry COUNT', 'automatic retrying count', Integer) {|i|
      retry_limit = i
    }
    op.on('-q', '--query PATH', 'use file instead of inline query') {|s|
      query = File.open(s) { |f| f.read.strip }
    }
    op.on('-t', '--type TYPE', 'set query type (hive or pig)') {|s|
      type = s.to_sym
    }
    op.on('--sampling DENOMINATOR', 'enable random sampling to reduce records 1/DENOMINATOR', Integer) {|i|
      sampling_all = i
    }

    sql = op.cmd_parse

    unless db_name
      $stderr.puts "-d, --database DB_NAME option is required."
      exit 1
    end

    if sql == '-'
      sql = STDIN.read
    elsif sql.nil?
      sql = query
    end

    unless sql
      $stderr.puts "<sql> argument or -q,--query PATH option is required."
      exit 1
    end

    if result_url
      require 'td/command/result'
      result_url = build_result_url(result_url, result_user, result_ask_password)
    end

    client = get_client

    # local existance check
    get_database(client, db_name)

    opts = {}
    opts['organization'] = org if org
    opts['sampling_all'] = sampling_all if sampling_all
    opts['type'] = type if type
    job = client.query(db_name, sql, result_url, priority, retry_limit, opts)

    $stderr.puts "Job #{job.job_id} is queued."
    $stderr.puts "Use '#{$prog} job:show #{job.job_id}' to show the status."
    #$stderr.puts "See #{job.url} to see the progress."

    if wait && !job.finished?
      wait_job(job)
      puts "Status     : #{job.status}"
      if job.success?
        puts "Result     :"
        show_result(job, output, format, render_opts)
      end
    end
  end

  require 'td/command/job'  # wait_job, job_priority_id_of
end
end

