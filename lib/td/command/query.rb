
module TD
module Command

  def query
    op = cmd_opt 'query', :sql

    op.banner << "\noptions:\n"

    db_name = nil
    op.on('-d', '--database DB_NAME', 'use the database (required)') {|s|
      db_name = s
    }

    wait = false
    op.on('-w', '--wait', 'wait for finishing the job', TrueClass) {|b|
      wait = b
    }

    sql = op.cmd_parse

    api = cmd_api

    unless db_name
      $stderr.puts "-d, --database DB_NAME option is required."
      exit 1
    end

    find_database(api, db_name)

    job = api.query(db_name, sql)

    $stderr.puts "Job #{job.job_id} is started."
    $stderr.puts "Use '#{$prog} job #{job.job_id}' to show the status."
    $stderr.puts "See #{job.url} to see the progress."

    if wait && !job.finished?
      wait_job(job)
      puts "Status     : #{job.status}"
      puts "Result     :"
      puts render_result(job.result)
    end
  end

  def show_jobs
    op = cmd_opt 'show-jobs', :max?

    op.banner << "\noptions:\n"

    page = 0
    skip = 0
    from = nil
    around = nil

    op.on('-p', '--page PAGE', 'skip N pages', Integer) {|i|
      page = i
    }
    op.on('-s', '--skip N', 'skip N jobs', Integer) {|i|
      skip = i
    }
    op.on('-f', '--from JOB_ID', 'show jobs from the id', Integer) {|i|
      from = i
    }
    op.on('-a', '--around JOB_ID', 'show jobs around the id', Integer) {|i|
      around = i
    }

    max = op.cmd_parse

    max = (max || 20).to_i

    api = cmd_api

    if from || around
      jobs = api.jobs(0, 1)
      if last = jobs[0]
        if from
          skip += last.job_id.to_i - from - (max-1)
        else
          skip += last.job_id.to_i - around - (max-1) + (max-1)/2
        end
      end
    end
    if page
      skip += max * page
    end
    jobs = api.jobs(skip, skip+max-1)

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
    op.on('-v', '--verbose', 'show logs', TrueClass) {|b|
      verbose = b
    }

    wait = false
    op.on('-w', '--wait', 'wait for finishing the job', TrueClass) {|b|
      wait = b
    }

    job_id = op.cmd_parse

    api = cmd_api

    job = api.job(job_id)

    puts "JobID      : #{job.job_id}"
    puts "URL        : #{job.url}"
    puts "Status     : #{job.status}"
    puts "Query      : #{job.query}"

    if wait && !job.finished?
      wait_job(job)
      puts "Result     :"
      puts render_result(job.result)

    else
      if job.finished?
        puts "Result     :"
        puts render_result(job.result)
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

  def render_result(result)
    require 'json'
    rows = result.map {|row|
      row.map {|v|
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

