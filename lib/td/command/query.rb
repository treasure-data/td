
module TreasureData
module Command

  def query
    op = get_option('query')

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

    get_database(client, db_name)

    job = client.query(db_name, sql)

    $stderr.puts "Job #{job.job_id} is started."
    $stderr.puts "Use '#{$prog} job:show #{job.job_id}' to show the status."
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

  require 'td/command/job'  # wait_job
end
end

