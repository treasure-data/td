require 'td/command/job'
require 'td/command/options'

module TreasureData
module Command

  def query(op)
    db_name = nil
    wait = false
    output = nil
    format = nil
    render_opts = {:header => false}
    result_url = nil
    result_user = nil
    result_ask_password = false
    priority = nil
    retry_limit = nil
    query = nil
    type = nil
    limit = nil
    exclude = false
    pool_name = nil
    domain_key = nil

    op.on('-d', '--database DB_NAME', 'use the database (required)') {|s|
      db_name = s
    }
    op.on('-w', '--wait[=SECONDS]', 'wait for finishing the job (for seconds)', Integer) {|b|
      wait = b
    }
    op.on('-G', '--vertical', 'use vertical table to show results', TrueClass) {|b|
      render_opts[:vertical] = b
    }
    op.on('-o', '--output PATH', 'write result to the file') {|s|
      unless Dir.exist?(File.dirname(s))
        s = File.expand_path(s)
      end
      output = s
      format = 'tsv' if format.nil?
    }

    TreasureData::Command::Options.write_format_option(op) {|s| format = s }

    op.on('-r', '--result RESULT_URL', 'write result to the URL (see also result:create subcommand)',
                                       ' It is suggested for this option to be used with the -x / --exclude option to suppress printing',
                                       ' of the query result to stdout or -o / --output to dump the query result into a file.') {|s|
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
    op.on('-T', '--type TYPE', 'set query type (hive, presto)') {|s|
      type = s.to_sym
    }
    op.on('--sampling DENOMINATOR', 'OBSOLETE - enable random sampling to reduce records 1/DENOMINATOR', Integer) {|i|
      $stdout.puts "WARNING: the random sampling feature enabled through the '--sampling' option was removed and does no longer"
      $stdout.puts "         have any effect. It is left for backwards compatibility with older scripts using 'td'."
      $stdout.puts
    }
    op.on('-l', '--limit ROWS', 'limit the number of result rows shown when not outputting to file') {|s|
      unless s.to_i > 0
        raise "Invalid limit number. Must be a positive integer"
      end
      limit = s.to_i
    }
    op.on('-c', '--column-header', 'output of the columns\' header when the schema is available for the table (only applies to json, tsv and csv formats)', TrueClass) {|b|
      render_opts[:header] = b
    }
    op.on('-x', '--exclude', 'do not automatically retrieve the job result', TrueClass) {|b|
      exclude = b
    }
    op.on('-O', '--pool-name NAME', 'specify resource pool by name') {|s|
      pool_name = s
    }
    op.on('--domain-key DOMAIN_KEY', 'optional user-provided unique ID. You can include this ID with your `create` request to ensure idempotence') {|s|
      domain_key = s
    }

    sql = op.cmd_parse

    # required parameters

    unless db_name
      raise ParameterConfigurationError,
            "-d / --database DB_NAME option is required."
    end

    if sql == '-'
      sql = STDIN.read
    elsif sql.nil?
      sql = query
    end
    unless sql
      raise ParameterConfigurationError,
            "<sql> argument or -q / --query PATH option is required."
    end

    # parameter concurrency validation

    if output.nil? && format
      unless ['tsv', 'csv', 'json'].include?(format)
        raise ParameterConfigurationError,
              "Supported formats are only tsv, csv and json without --output option"
      end
    end

    if render_opts[:header]
      unless ['tsv', 'csv', 'json'].include?(format)
        raise ParameterConfigurationError,
              "Option -c / --column-header is only supported with json, tsv and csv formats"
      end
    end

    if result_url
      require 'td/command/result'
      result_url = build_result_url(result_url, result_user, result_ask_password)
    end

    client = get_client

    # local existence check
    get_database(client, db_name)

    opts = {}
    opts['type'] = type if type
    opts['pool_name'] = pool_name if pool_name
    opts['domain_key'] = domain_key if domain_key
    job = client.query(db_name, sql, result_url, priority, retry_limit, opts)

    $stdout.puts "Job #{job.job_id} is queued."
    $stdout.puts "Use '#{$prog} " + Config.cl_options_string + "job:show #{job.job_id}' to show the status."
    #puts "See #{job.url} to see the progress."

    if wait != false # `wait==nil` means `--wait` is specified
      wait_job(job, true, wait)
      $stdout.puts "Status      : #{job.status}"
      if job.success? && !exclude
        begin
          show_result_with_retry(job, output, limit, format, render_opts)
        rescue TreasureData::NotFoundError => e
        end
      end
    end
  end

  require 'td/command/job'  # wait_job, job_priority_id_of
end
end
