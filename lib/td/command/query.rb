
module TreasureData
module Command

  def query
    op = cmd_opt 'query', :sql

    op.banner << "\noptions:\n"

    db_name = nil
    wait = false
    output = nil
    format = 'tsv'

    op.on('-d', '--database DB_NAME', 'use the database (required)') {|s|
      db_name = s
    }
    op.on('-w', '--wait', 'wait for finishing the job', TrueClass) {|b|
      wait = b
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

    sql = op.cmd_parse

    client = get_client

    unless db_name
      $stderr.puts "-d, --database DB_NAME option is required."
      exit 1
    end

    find_database(client, db_name)

    job = client.query(db_name, sql)

    $stderr.puts "Job #{job.job_id} is started."
    $stderr.puts "Use '#{$prog} job #{job.job_id}' to show the status."
    $stderr.puts "See #{job.url} to see the progress."

    if wait && !job.finished?
      wait_job(job)
      puts "Status     : #{job.status}"
      if job.success?
        puts "Result     :"
        show_result(job, output, format)
      end
    end
  end

  def show_jobs
    op = cmd_opt 'show-jobs', :max?

    op.banner << "\noptions:\n"

    page = 0
    skip = 0

    op.on('-p', '--page PAGE', 'skip N pages', Integer) {|i|
      page = i
    }
    op.on('-s', '--skip N', 'skip N jobs', Integer) {|i|
      skip = i
    }

    max = op.cmd_parse

    max = (max || 20).to_i

    client = get_client

    if page
      skip += max * page
    end
    jobs = client.jobs(skip, skip+max-1)

    rows = []
    jobs.each {|job|
      start = job.start_at
      finish = job.end_at
      if start
        if !finish
          finish = Time.now.utc
        end
        e = finish.to_i - start.to_i
        elapsed = ''
        if e >= 3600
          elapsed << "#{e/3600}h "
          e %= 3600
          elapsed << "% 2dm " % (e/60)
          e %= 60
          elapsed << "% 2dsec" % e
        elsif e >= 60
          elapsed << "% 2dm " % (e/60)
          e %= 60
          elapsed << "% 2dsec" % e
        else
          elapsed << "% 2dsec" % e
        end
      else
        elapsed = ''
      end
      elapsed = "% 10s" % elapsed  # right aligned

      rows << {:JobID => job.job_id, :Status => job.status, :Query => job.query.to_s, :Start => start, :Elapsed => elapsed}
    }

    puts cmd_render_table(rows, :fields => [:JobID, :Status, :Start, :Elapsed, :Query])
  end

  def job
    op = cmd_opt 'job', :job_id

    op.banner << "\noptions:\n"

    verbose = nil
    wait = false
    output = nil
    format = 'tsv'

    op.on('-v', '--verbose', 'show logs', TrueClass) {|b|
      verbose = b
    }
    op.on('-w', '--wait', 'wait for finishing the job', TrueClass) {|b|
      wait = b
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

    job_id = op.cmd_parse

    client = get_client

    job = client.job(job_id)

    puts "JobID      : #{job.job_id}"
    puts "URL        : #{job.url}"
    puts "Status     : #{job.status}"
    puts "Query      : #{job.query}"

    if wait && !job.finished?
      wait_job(job)
      if job.success?
        puts "Result     :"
        show_result(job, output, format)
      end

    else
      if job.success?
        puts "Result     :"
        show_result(job, output, format)
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

  private
  def wait_job(job)
    $stderr.puts "running..."

    cmdout_lines = 0
    stderr_lines = 0

    until job.finished?
      sleep 2

      job.update_status!

      cmdout = job.debug['cmdout'].to_s.split("\n")[cmdout_lines..-1] || []
      stderr = job.debug['stderr'].to_s.split("\n")[stderr_lines..-1] || []
      (cmdout + stderr).each {|line|
        puts "  "+line
      }
      cmdout_lines += cmdout.size
      stderr_lines += stderr.size
    end
  end

  def show_result(job, output, format)
    if output
      write_result(job, output, format)
      puts "written to #{output} in #{format} format"
    else
      render_result(job)
    end
  end

  def write_result(job, output, format)
    case format
    when 'json'
      require 'json'
      first = true
      File.open(output, "w") {|f|
        f.write "["
        job.result_each {|row|
          if first
            first = false
          else
            f.write ","
          end
          f.write row.to_json
        }
        f.write "]"
      }

    when 'msgpack'
      File.open(output, "w") {|f|
        f.write job.result_format('msgpack')
      }

    when 'csv'
      require 'json'
      require 'csv'
      CSV.open(output, "w") {|writer|
        job.result_each {|row|
          writer << row.map {|col| col.is_a?(String) ? col.to_s : col.to_json }
        }
      }

    when 'tsv'
      require 'json'
      File.open(output, "w") {|f|
        job.result_each {|row|
          first = true
          row.each {|col|
            if first
              first = false
            else
              f.write "\t"
            end
            f.write col.is_a?(String) ? col.to_s : col.to_json
          }
          f.write "\n"
        }
      }

    else
      raise "Unknown format #{format.inspect}"
    end
  end

  def render_result(job)
    require 'json'
    rows = []
    job.result_each {|row|
      # TODO limit number of rows to show
      rows << row.map {|v|
        if v.is_a?(String)
          v.to_s
        else
          v.to_json
        end
      }
    }
    puts cmd_render_table(rows, :max_width=>10000)
  end
end
end

