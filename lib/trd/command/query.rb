
module TRD
module Command

  def query
    op = cmd_opt 'query', :sql

    op.banner << "\noptions:\n"

    db_name = nil
    op.on('-d', '--database DB_NAME', 'use the database') {|s|
      db_name = s
    }

    sql = op.cmd_parse

    conf = cmd_config
    api = cmd_api(conf)

    if db_name
      find_database(api, db_name)
    end

    job = api.query(sql, db_name)

    $stderr.puts "Job #{job.job_id} is started."
    $stderr.puts "Use '#{$prog} job #{job.job_id}' to show the status."
    $stderr.puts "See #{job.url} to see the progress."
  end

  def show_jobs
    op = cmd_opt 'show-jobs', :db_name?, :max?, :from?
    db_name, max, from = op.cmd_parse

    max = (max || 19).to_i
    from = (from || 0).to_i

    conf = cmd_config
    api = cmd_api(conf)

    jobs = api.jobs(from, from+max)

    rows = []
    jobs.each {|job|
      rows << {:JobID => job.job_id, :Status => job.status}
    }

    puts cmd_render_table(rows, :fields => [:JobID, :Status])
  end

  def job
    op = cmd_opt 'job', :job_id

    op.banner << "\noptions:\n"

    verbose = nil
    op.on('-v', '--verbose', 'show logs', TrueClass) {|b|
      verbose = b
    }

    job_id = op.cmd_parse

    conf = cmd_config
    api = cmd_api(conf)

    job = api.job(job_id)

    puts "JobID      : #{job.job_id}"
    puts "URL        : #{job.url}"
    puts "Status     : #{job.status}"

    if job.finished?
      puts "Result     :"
      puts cmd_render_table(job.result)
      #cmd_render_table(job.result).split("\n").each {|line|
      #  puts line
      #}
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

    $stderr.puts "Use '-v' option to show detailed messages." unless verbose
  end

end
end

