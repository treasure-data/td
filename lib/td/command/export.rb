require 'td/command/job'

module TreasureData
module Command
  SUPPORTED_FORMATS = %W[json.gz line-json.gz tsv.gz jsonl.gz]
  SUPPORTED_ENCRYPT_METHOD = %W[s3]

  def export_result(op)
    wait = false
    priority = nil
    retry_limit = nil

    op.on('-w', '--wait', 'wait until the job is completed', TrueClass) {|b|
      wait = b
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

    target_job_id, result = op.cmd_parse

    client = get_ssl_client

    opts = {
      result: result,
      retry_limit: retry_limit,
      priority: priority,
    }
    if wait
      job = client.job(target_job_id)
      if !job.finished?
        $stderr.puts "target job #{target_job_id} is still running..."
        wait_job(job)
      end
    end

    job = client.result_export(target_job_id, opts)
    $stderr.puts "result export job #{job.job_id} is queued."
    $stderr.puts "Use '#{$prog} " + Config.cl_options_string + "job:show #{job.job_id}' to show the status."

    if wait
      wait_job(job)
      $stdout.puts "status     : #{job.status}"
    end
  end

  def export_table(op)
    table_export(op)
  end

  def table_export(op)
    from = nil
    to = nil
    s3_bucket = nil
    wait = false
    aws_access_key_id = nil
    aws_secret_access_key = nil
    file_prefix = nil
    file_format = "json.gz" # default
    pool_name = nil
    encryption = nil
    assume_role = nil

    op.on('-w', '--wait', 'wait until the job is completed', TrueClass) {|b|
      wait = b
    }
    op.on('-f', '--from TIME', 'export data which is newer than or same with the TIME') {|s|
      from = export_parse_time(s)
    }
    op.on('-t', '--to TIME', 'export data which is older than the TIME') {|s|
      to = export_parse_time(s)
    }
    op.on('-b', '--s3-bucket NAME', 'name of the destination S3 bucket (required)') {|s|
      s3_bucket = s
    }
    op.on('-p', '--prefix PATH', 'path prefix of the file on S3') {|s|
      file_prefix = s
    }
    op.on('-k', '--aws-key-id KEY_ID', 'AWS access key id to export data (required)') {|s|
      aws_access_key_id = s
    }
    op.on('-s', '--aws-secret-key SECRET_KEY', 'AWS secret access key to export data (required)') {|s|
      aws_secret_access_key = s
    }
    op.on('-F', '--file-format FILE_FORMAT',
          'file format for exported data.',
          'Available formats are tsv.gz (tab-separated values per line) and jsonl.gz (JSON record per line).',
          'The json.gz and line-json.gz formats are default and still available but only for backward compatibility purpose;',
          '  use is discouraged because they have far lower performance.') { |s|
      raise ArgumentError, "#{s} is not a supported file format" unless SUPPORTED_FORMATS.include?(s)
      file_format = s
    }
    op.on('-O', '--pool-name NAME', 'specify resource pool by name') {|s|
      pool_name = s
    }
    op.on('-e', '--encryption ENCRYPT_METHOD', 'export with server side encryption with the ENCRYPT_METHOD') {|s|
      raise ArgumentError, "#{s} is not a supported encryption method" unless SUPPORTED_ENCRYPT_METHOD.include?(s)
      encryption = s
    }
    op.on('-a', '--assume-role ASSUME_ROLE_ARN', 'export with assume role with ASSUME_ROLE_ARN as role arn') {|s|
      assume_role = s
    }

    db_name, table_name = op.cmd_parse

    unless s3_bucket
      $stderr.puts "-b, --s3-bucket NAME option is required."
      exit 1
    end

    unless aws_access_key_id
      $stderr.puts "-k, --aws-key-id KEY_ID option is required."
      exit 1
    end

    unless aws_secret_access_key
      $stderr.puts "-s, --aws-secret-key SECRET_KEY option is required."
      exit 1
    end

    client = get_client

    get_table(client, db_name, table_name)

    client = get_ssl_client

    s3_opts = {}
    s3_opts['from'] = from.to_s if from
    s3_opts['to'] = to.to_s if to
    s3_opts['file_prefix'] = file_prefix if file_prefix
    s3_opts['file_format'] = file_format
    s3_opts['bucket'] = s3_bucket
    s3_opts['access_key_id'] = aws_access_key_id
    s3_opts['secret_access_key'] = aws_secret_access_key
    s3_opts['pool_name'] = pool_name if pool_name
    s3_opts['encryption'] = encryption if encryption
    s3_opts['assume_role'] = assume_role if assume_role

    job = client.export(db_name, table_name, "s3", s3_opts)

    $stderr.puts "Export job #{job.job_id} is queued."
    $stderr.puts "Use '#{$prog} " + Config.cl_options_string + "job:show #{job.job_id}' to show the status."

    if wait
      wait_job(job)
      $stdout.puts "Status     : #{job.status}"
    end
  end

  private
  def export_parse_time(time)
    if time.to_i.to_s == time.to_s
      # UNIX time
      return time.to_i
    else
      require 'time'
      begin
        return Time.parse(time).to_i
      rescue
        $stderr.puts "invalid time format: #{time}"
        exit 1
      end
    end
  end

end
end

