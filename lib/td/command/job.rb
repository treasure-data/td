require 'td/command/options'

module TreasureData
module Command
  include Options

  # TODO
  JOB_WAIT_MAX_RETRY_COUNT_ON_NETWORK_ERROR = 10

  PRIORITY_FORMAT_MAP = {
    -2 => 'VERY LOW',
    -1 => 'LOW',
    0 => 'NORMAL',
    1 => 'HIGH',
    2 => 'VERY HIGH',
  }

  PRIORITY_PARSE_MAP = {
    /\Avery[ _\-]?low\z/i => -2,
    /\A-2\z/ => -2,
    /\Alow\z/i => -1,
    /\A-1\z/ => -1,
    /\Anorm(?:al)?\z/i => 0,
    /\A[\-\+]?0\z/ => 0,
    /\Ahigh\z/i => 1,
    /\A[\+]?1\z/ => 1,
    /\Avery[ _\-]?high\z/i => 2,
    /\A[\+]?2\z/ => 2,
  }

  def job_list(op)
    page = 0
    skip = 0
    status = nil
    slower_than = nil

    op.on('-p', '--page PAGE', 'skip N pages', Integer) {|i|
      page = i
    }
    op.on('-s', '--skip N', 'skip N jobs', Integer) {|i|
      skip = i
    }
    op.on('-R', '--running', 'show only running jobs', TrueClass) {|b|
      status = 'running'
    }
    op.on('-S', '--success', 'show only succeeded jobs', TrueClass) {|b|
      status = 'success'
    }
    op.on('-E', '--error', 'show only failed jobs', TrueClass) {|b|
      status = 'error'
    }
    op.on('--slow [SECONDS]', 'show slow queries (default threshold: 3600 seconds)', Integer) {|i|
      slower_than = i || 3600
    }

    set_render_format_option(op)

    max = op.cmd_parse

    max = (max || 20).to_i

    client = get_client

    if page
      skip += max * page
    end

    conditions = nil
    if slower_than
      conditions = {:slower_than => slower_than}
    end

    jobs = client.jobs(skip, skip + max - 1, status, conditions)

    rows = []
    jobs.each {|job|
      job.auto_update_status = false
      start = job.start_at
      elapsed = Command.humanize_elapsed_time(start, job.end_at)
      cpu_time = Command.humanize_time(job.cpu_time, true)
      priority = job_priority_name_of(job.priority)
      query = (op.render_format == 'table' || op.render_format.nil? ? job.query.to_s[0,50] + " ..." : job.query)
      rows << {
        :JobID => job.job_id,
        :Database => job.db_name,
        :Status => job.status,
        :Type => job.type,
        :Query => query,
        :Start => (start ? start.localtime : ''),
        :Elapsed => elapsed.rjust(11),
        :CPUTime => cpu_time.rjust(17),
        :ResultSize => (job.result_size ? Command.humanize_bytesize(job.result_size, 2) : ""),
        :Priority => priority,
        :Result => job.result_url,
        :Duration => job.duration ? Time.at(job.duration).utc.strftime("%X") : nil
      }
    }

    $stdout.puts cmd_render_table(rows,
      :fields => [:JobID, :Status, :Start, :Elapsed, :CPUTime, :ResultSize, :Priority, :Result, :Type, :Database, :Query, :Duration],
      :max_width => 1000,
      :render_format => op.render_format
    )
  end

  def job_show(op)
    options = job_show_options(op)
    job_id = op.cmd_parse

    verbose     = options[:verbose]
    wait        = options[:wait]
    output      = options[:output]
    format      = options[:format]
    render_opts = options[:render_opts]
    limit       = options[:limit]
    exclude     = options[:exclude]

    if output.nil? && format
      unless ['tsv', 'csv', 'json'].include?(format)
        raise ParameterConfigurationError,
              "Supported formats are only tsv, csv and json without -o / --output option"
      end
    end

    if render_opts[:header]
      unless ['json', 'tsv', 'csv'].include?(format)
        raise ParameterConfigurationError,
              "Option -c / --column-header is only supported with json, tsv and csv formats"
      end
    end

    if !output.nil? && !limit.nil?
      raise ParameterConfigurationError,
            "Option -l / --limit is only valid when not outputting to file (no -o / --output option provided)"
    end

    get_and_show_result(job_id, wait, exclude, output, limit, format, render_opts, verbose)
  end

  def job_status(op)
    job_id = op.cmd_parse
    client = get_client

    $stdout.puts client.job_status(job_id)
  end

  def job_kill(op)
    job_id = op.cmd_parse

    client = get_client

    former_status = client.kill(job_id)
    if TreasureData::Job::FINISHED_STATUS.include?(former_status)
      $stderr.puts "Job #{job_id} is already finished (#{former_status})"
      exit 0
    end

    if former_status == TreasureData::Job::STATUS_RUNNING
      $stderr.puts "Job #{job_id} is killed."
    else
      $stderr.puts "Job #{job_id} is canceled."
    end
  end

private

  def get_and_show_result(job_id, wait, exclude = false, output = nil, limit = nil, format = nil, render_opts = {}, verbose = false)
    client = get_client
    job = client.job(job_id)

    $stdout.puts "JobID       : #{job.job_id}"
    #puts "URL         : #{job.url}"
    $stdout.puts "Status      : #{job.status}"
    $stdout.puts "Type        : #{job.type}"
    $stdout.puts "Database    : #{job.db_name}"
    # exclude some fields from bulk_import_perform type jobs
    if [:hive, :pig, :impala, :presto].include?(job.type)
      $stdout.puts "Priority    : #{job_priority_name_of(job.priority)}"
      $stdout.puts "Retry limit : #{job.retry_limit}"
      $stdout.puts "Output      : #{job.result_url}"
      $stdout.puts "Query       : #{job.query}"
    elsif job.type == :bulk_import_perform
      $stdout.puts "Destination : #{job.query}"
    elsif job.type == :bulkload
      require 'yaml'
      $stdout.puts "Config      :\n#{YAML.dump(job.query)}"
    end
    # if the job is done and is of type hive, show the Map-Reduce cumulated CPU time
    if job.finished?
      if [:hive].include?(job.type)
        $stdout.puts "CPU time    : #{Command.humanize_time(job.cpu_time, true)}"
      end
      if [:hive, :pig, :impala, :presto].include?(job.type)
        $stdout.puts "Result size : #{Command.humanize_bytesize(job.result_size, 2)}"
      end
    end

    if wait && !job.finished?
      $stderr.puts "the job #{job.job_id} is still running..."
      wait_job(job)
      if [:hive, :pig, :impala, :presto].include?(job.type) && !exclude
        show_result_with_retry(job, output, limit, format, render_opts)
      end
    else
      if [:hive, :pig, :impala, :presto].include?(job.type) && !exclude && job.finished?
        show_result_with_retry(job, output, limit, format, render_opts)
      end

      if verbose
        if !job.debug['cmdout'].nil?
          $stdout.puts ""
          $stdout.puts "Output:"
          job.debug['cmdout'].to_s.split("\n").each {|line|
            $stdout.puts "  " + line
          }
        end
        if !job.debug['stderr'].nil?
          $stdout.puts ""
          $stdout.puts "Details:"
          job.debug['stderr'].to_s.split("\n").each {|line|
            $stdout.puts "  " + line
          }
        end
      end
    end

    $stdout.puts "\rUse '-v' option to show detailed messages." + " " * 20 unless verbose
  end

  def wait_job(job, first_call = false, wait = nil)
    cmdout_lines = 0
    stderr_lines = 0
    max_error_counts = JOB_WAIT_MAX_RETRY_COUNT_ON_NETWORK_ERROR

    job.wait(wait, detail: true, verbose: true) do
      cmdout = job.debug['cmdout'].to_s.split("\n")[cmdout_lines..-1] || []
      stderr = job.debug['stderr'].to_s.split("\n")[stderr_lines..-1] || []
      (cmdout + stderr).each {|line|
        $stdout.puts "  "+line
      }
      cmdout_lines += cmdout.size
      stderr_lines += stderr.size
    end
  end

  def show_result_with_retry(job, output, limit, format, render_opts)
    # up to 7 retries with exponential (base 2) back-off starting at 'retry_delay'
    retry_delay = 5
    max_cumul_retry_delay = 200
    cumul_retry_delay = 0

    $stdout.puts "Result      :"
    begin
      show_result(job, output, limit, format, render_opts)
    rescue TreasureData::NotFoundError => e
      # Got 404 because result not found.
    rescue TreasureData::APIError, # HTTP status code 500 or more
            Errno::ECONNREFUSED, Errno::ECONNRESET, Timeout::Error, EOFError,
            OpenSSL::SSL::SSLError, SocketError => e
      # don't retry on 300 and 400 errors
      if e.class == TreasureData::APIError && e.message !~ /^5\d\d:\s+/
        raise e
      end
      if cumul_retry_delay > max_cumul_retry_delay
        raise e
      end
      $stderr.puts "Error #{e.class}: #{e.message}. Retrying after #{retry_delay} seconds..."
      sleep retry_delay
      cumul_retry_delay += retry_delay
      retry_delay *= 2
      retry
    end
  end

  def show_result(job, output, limit, format, render_opts={})
    if output
      write_result(job, output, limit, format, render_opts)
      $stdout.puts "\rwritten to #{output} in #{format} format" + " " * 50
    else
      # every format that is allowed on stdout
      render_result(job, limit, format, render_opts)
    end
  end

  def write_result(job, output, limit, format, render_opts={})

    # the next 3 formats allow writing to both a file and stdout


    if output
      if output.is_a?(String)
        tempfile = "#{output}.tmp"
      else # File or Tempfile
        tempfile = "#{output.path}.tmp"
      end
    end

    case format
    when 'json'
      if render_opts[:header] && job.hive_result_schema
        headers = job.hive_result_schema.map {|name, _| name }

        write_result_for_json(job, output, tempfile, limit, render_opts) {|row|
          Hash[headers.zip(row)]
        }
      else
        write_result_for_json(job, output, tempfile, limit, render_opts) {|row| row }
      end
    when 'csv'
      require 'yajl'
      require 'csv'

      open_file(tempfile || output, "w") {|f|
        writer = CSV.new(f)
        # output headers
        if render_opts[:header] && job.hive_result_schema
          writer << job.hive_result_schema.map {|name, type|
            name
          }
        end
        # output data
        n_rows = 0
        unless output.nil?
          indicator = Command::SizeBasedDownloadProgressIndicator.new(
            "NOTE: the job result is being written to #{output} in csv format",
            job.result_size, 0.1, 1)
        end
        job.result_each_with_compr_size {|row, compr_size|
          # TODO limit the # of columns
          writer << row.map {|col|
            dump_column(col, render_opts[:null_expr])
          }
          n_rows += 1
          if n_rows % 100 == 0 # flush every 100 recods
            writer.flush
            indicator.update(compr_size) unless output.nil?
          end
          break if output.nil? and !limit.nil? and n_rows == limit
        }
        indicator.finish unless output.nil?
      }

    when 'tsv'
      require 'yajl'

      open_file(tempfile || output, "w") {|f|
        # output headers
        if render_opts[:header] && job.hive_result_schema
          f.write job.hive_result_schema.map {|name, type| name}.join("\t") + "\n"
        end
        # output data
        n_rows = 0
        unless output.nil?
          indicator = Command::SizeBasedDownloadProgressIndicator.new(
            "NOTE: the job result is being written to #{output} in tsv format",
            job.result_size, 0.1, 1)
        end

        job.result_each_with_compr_size {|row, compr_size|
          f.write row.map {|col| dump_column(col, render_opts[:null_expr])}.join("\t") + "\n"
          n_rows += 1
          if n_rows % 100 == 0
            f.flush # flush every 100 recods
            indicator.update(compr_size) unless output.nil?
          end
          break if output.nil? and !limit.nil? and n_rows == limit
        }
        indicator.finish unless output.nil?
      }

    # these last 2 formats are only valid if writing the result to file through the -o/--output option.

    when 'msgpack'
      if output.nil?
        raise ParameterConfigurationError,
              "Format 'msgpack' does not support writing to stdout"
      end
      open_file(tempfile || output, "wb") {|f|
        indicator = Command::SizeBasedDownloadProgressIndicator.new(
          "NOTE: the job result is being written to #{output} in msgpack format",
          job.result_size, 0.1, 1)
        job.result_format('msgpack', f) {|compr_size|
          indicator.update(compr_size)
        }
        indicator.finish
      }

    when 'msgpack.gz'
      if output.nil?
        raise ParameterConfigurationError,
              "Format 'msgpack' does not support writing to stdout"
      end
      open_file(tempfile || output, "wb") {|f|
        indicator = Command::SizeBasedDownloadProgressIndicator.new(
          "NOTE: the job result is being written to #{output} in msgpack.gz format",
          job.result_size, 0.1, 1)
        job.result_raw('msgpack.gz', f) {|compr_size|
          indicator.update(compr_size)
        }
        indicator.finish
      }

    else
      raise "Unknown format #{format.inspect}"
    end

    if tempfile && File.exists?(tempfile)
      FileUtils.mv(tempfile, output.respond_to?(:path) ? output.path : output)
    end
  end

  def open_file(output, mode)
    f = nil
    if output.nil?
      yield STDOUT
    else
      f = File.open(output, mode)
      yield f
    end
  ensure
    if f
      f.close unless f.closed?
    end
  end

  def write_result_for_json(job, output, tempfile, limit, render_opts)
    require 'yajl'
    open_file(tempfile || output, "w") {|f|
      f.write "["
      n_rows = 0
      unless output.nil?
        indicator = Command::SizeBasedDownloadProgressIndicator.new(
          "NOTE: the job result is being written to #{output} in json format",
          job.result_size, 0.1, 1)
      end
      job.result_each_with_compr_size {|row, compr_size|
        indicator.update(compr_size) unless output.nil?
        f.write ",\n" if n_rows > 0
        f.write Yajl.dump(yield(row))
        n_rows += 1
        break if output.nil? and !limit.nil? and n_rows == limit
      }
      f.write "]"
      indicator.finish unless output.nil?
    }
    $stdout.puts if output.nil?
  end

  def render_result(job, limit, format=nil, render_opts={})
    require 'yajl'

    if format.nil?
      # display result in tabular format
      rows = []
      n_rows = 0

      indicator = Command::SizeBasedDownloadProgressIndicator.new(
        "WARNING: the job result is being downloaded...", job.result_size, 0.1, 1)
      job.result_each_with_compr_size {|row, compr_size|
        indicator.update(compr_size)
        rows << row.map {|v|
          dump_column_safe_utf8(v, render_opts[:null_expr])
        }
        n_rows += 1
        break if !limit.nil? and n_rows == limit
      }
      $stdout.print " " * 100, "\r" # make sure the previous WARNING is cleared over

      render_opts[:max_width] = 10000
      if job.hive_result_schema
        render_opts[:change_fields] = job.hive_result_schema.map { |name,type| name }
      end

      $stdout.print "\r" + " " * 50
      $stdout.puts "\r" + cmd_render_table(rows, render_opts)
    else
      # display result in any of: json, csv, tsv.
      # msgpack and mspgpack.gz are not supported for stdout output
      write_result(job, nil, limit, format, render_opts)
    end
  end

  def dump_column(v, null_expr = nil)
    v = null_expr if v.nil? && null_expr

    s = v.is_a?(String) ? v.to_s : Yajl.dump(sanitize_infinite_value(v))
    # CAUTION: msgpack-ruby populates byte sequences as Encoding.default_internal which should be BINARY
    s = s.force_encoding('BINARY') if s.respond_to?(:encode)
    s
  end

  def dump_column_safe_utf8(v, null_expr = false)
    s = dump_column(v, null_expr)
    # Here does UTF-8 -> UTF-16LE -> UTF8 conversion:
    #   a) to make sure the string doesn't include invalid byte sequence
    #   b) to display multi-byte characters as it is
    #   c) encoding from UTF-8 to UTF-8 doesn't check/replace invalid chars
    #   d) UTF-16LE was slightly faster than UTF-16BE, UTF-32LE or UTF-32BE
    s = s.encode('UTF-16LE', 'UTF-8', :invalid=>:replace, :undef=>:replace).encode!('UTF-8') if s.respond_to?(:encode)
    s
  end

  def sanitize_infinite_value(v)
    case v
    when Float
      v.finite? ? v : v.to_s
    when Hash, Array
      Marshal.load(Marshal.dump(v), ->(x){(x.is_a?(Float) && !x.finite?) ? x.to_s : x})
    else
      v
    end
  end

  def job_priority_name_of(id)
    PRIORITY_FORMAT_MAP[id] || 'NORMAL'
  end

  def job_priority_id_of(name)
    PRIORITY_PARSE_MAP.each_pair {|pattern,id|
      return id if pattern.match(name)
    }
    return nil
  end

end # module Command
end # module TrasureData
