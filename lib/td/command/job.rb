
module TreasureData
module Command

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
    op.on('--slow [SECONDS]', 'show slow queries (default threshold: 3600 seconds)', Integer) { |i|
      slower_than = i || 3600
    }

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

    jobs = client.jobs(skip, skip+max-1, status, conditions)

    rows = []
    has_org = false
    jobs.each {|job|
      start = job.start_at
      elapsed = cmd_format_elapsed(start, job.end_at)
      priority = job_priority_name_of(job.priority)
      rows << {:JobID => job.job_id, :Database => job.db_name, :Status => job.status, :Type => job.type, :Query => job.query.to_s, :Start => (start ? start.localtime : ''), :Elapsed => elapsed, :Priority => priority, :Result => job.result_url, :Organization => job.org_name}
      has_org = true if job.org_name
    }

    puts cmd_render_table(rows, :fields => gen_table_fields(has_org, [:JobID, :Status, :Start, :Elapsed, :Priority, :Result, :Type, :Database, :Query]), :max_width => 140)
  end

  def job_show(op)
    verbose = nil
    wait = false
    output = nil
    format = 'tsv'
    render_opts = {}

    op.on('-v', '--verbose', 'show logs', TrueClass) {|b|
      verbose = b
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
      unless ['tsv', 'csv', 'json', 'msgpack', 'msgpack.gz'].include?(s)
        raise "Unknown format #{s.dump}. Supported format: tsv, csv, json, msgpack, msgpack.gz"
      end
      format = s
    }

    job_id = op.cmd_parse

    client = get_client

    job = client.job(job_id)

    puts "Organization : #{job.org_name}"
    puts "JobID        : #{job.job_id}"
    #puts "URL          : #{job.url}"
    puts "Status       : #{job.status}"
    puts "Type         : #{job.type}"
    puts "Priority     : #{job_priority_name_of(job.priority)}"
    puts "Retry limit  : #{job.retry_limit}"
    puts "Result       : #{job.result_url}"
    puts "Database     : #{job.db_name}"
    puts "Query        : #{job.query}"

    if wait && !job.finished?
      wait_job(job)
      if job.success? && [:hive, :pig].include?(job.type)
        puts "Result       :"
        show_result(job, output, format, render_opts)
      end

    else
      if job.success? && [:hive, :pig].include?(job.type)
        puts "Result       :"
        show_result(job, output, format, render_opts)
      end

      if verbose
        puts ""
        puts "cmdout:"
        job.debug['cmdout'].to_s.split("\n").each {|line|
          puts "  "+line
        }
        puts ""
        puts "stderr:"
        job.debug['stderr'].to_s.split("\n").each {|line|
          puts "  "+line
        }
      end
    end

    $stderr.puts "Use '-v' option to show detailed messages." unless verbose
  end

  def job_status(op)
    job_id = op.cmd_parse
    client = get_client

    puts client.job_status(job_id)
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
  def wait_job(job)
    $stderr.puts "queued..."

    cmdout_lines = 0
    stderr_lines = 0
    max_error_counts = JOB_WAIT_MAX_RETRY_COUNT_ON_NETWORK_ERROR

    until job.finished?
      begin
        sleep 2
        job.update_status!
      rescue Timeout::Error, SystemCallError, EOFError, SocketError
        if max_error_counts <= 0
          raise
        end
        max_error_counts -= 1
        retry
      end

      cmdout = job.debug['cmdout'].to_s.split("\n")[cmdout_lines..-1] || []
      stderr = job.debug['stderr'].to_s.split("\n")[stderr_lines..-1] || []
      (cmdout + stderr).each {|line|
        puts "  "+line
      }
      cmdout_lines += cmdout.size
      stderr_lines += stderr.size
    end
  end

  def show_result(job, output, format, render_opts={})
    if output
      write_result(job, output, format)
      puts "written to #{output} in #{format} format"
    else
      render_result(job, render_opts)
    end
  end

  def write_result(job, output, format)
    case format
    when 'json'
      require 'yajl'
      first = true
      File.open(output, "w") {|f|
        f.write "["
        job.result_each {|row|
          if first
            first = false
          else
            f.write ","
          end
          f.write Yajl.dump(row)
        }
        f.write "]"
      }

    when 'msgpack'
      File.open(output, "wb") {|f|
        job.result_format('msgpack', f)
      }

    when 'msgpack.gz'
      File.open(output, "wb") {|f|
        job.result_format('msgpack.gz', f)
      }

    when 'csv'
      require 'yajl'
      require 'csv'
      CSV.open(output, "w") {|writer|
        job.result_each {|row|
          writer << row.map {|col| col.is_a?(String) ? col.to_s : Yajl.dump(col) }
        }
      }

    when 'tsv'
      require 'yajl'
      File.open(output, "w") {|f|
        job.result_each {|row|
          first = true
          row.each {|col|
            if first
              first = false
            else
              f.write "\t"
            end
            f.write col.is_a?(String) ? col.to_s : Yajl.dump(col)
          }
          f.write "\n"
        }
      }

    else
      raise "Unknown format #{format.inspect}"
    end
  end

  def render_result(job, opts)
    require 'yajl'
    rows = []
    job.result_each {|row|
      # TODO limit number of rows to show
      rows << row.map {|v|
        if v.is_a?(String)
          s = v.to_s
        else
          s = Yajl.dump(v)
        end
        # Here does UTF-8 -> UTF-16LE -> UTF8 conversion:
        #   a) to make sure the string doesn't include invalid byte sequence
        #   b) to display multi-byte characters as it is
        #   c) encoding from UTF-8 to UTF-8 doesn't check/replace invalid chars
        #   d) UTF-16LE was slightly faster than UTF-16BE, UTF-32LE or UTF-32BE
        s = s.encode('UTF-16LE', 'UTF-8', :invalid=>:replace, :undef=>:replace).encode!('UTF-8') if s.respond_to?(:encode)
        s
      }
    }

    opts[:max_width] = 10000
    if job.hive_result_schema
      opts[:change_fields] = job.hive_result_schema.map {|name,type| name }
    end

    puts cmd_render_table(rows, opts)
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
end
end

