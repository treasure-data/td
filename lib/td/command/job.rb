
module TreasureData
module Command

  def job_list(op)
    page = 0
    skip = 0
    status = nil

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

    max = op.cmd_parse

    max = (max || 20).to_i

    client = get_client

    if page
      skip += max * page
    end
    jobs = client.jobs(skip, skip+max-1, status)

    rows = []
    jobs.each {|job|
      start = job.start_at
      elapsed = cmd_format_elapsed(start, job.end_at)
      rows << {:JobID => job.job_id, :Status => job.status, :Query => job.query.to_s, :Start => (start ? start.localtime : ''), :Elapsed => elapsed, :Result => job.result_url}
    }

    puts cmd_render_table(rows, :fields => [:JobID, :Status, :Start, :Elapsed, :Result, :Query])
  end

  def job_show(op)
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

    puts "JobID        : #{job.job_id}"
    #puts "URL          : #{job.url}"
    puts "Status       : #{job.status}"
    puts "Query        : #{job.query}"
    puts "Result table : #{job.result_url}"

    if wait && !job.finished?
      wait_job(job)
      if job.success? && job.type == :hive
        puts "Result       :"
        show_result(job, output, format)
      end

    else
      if job.success? && job.type == :hive
        puts "Result       :"
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
          # TODO encoding check
          s = v.to_s
          s.force_encoding('ASCII-8BIT') if s.respond_to?(:force_encoding)
          s
        else
          v.to_json
        end
      }
    }

    opts = {}
    opts[:max_width] = 10000
    if job.hive_result_schema
      opts[:change_fields] = job.hive_result_schema.map {|name,type| name }
    end

    puts cmd_render_table(rows, opts)
  end
end
end

