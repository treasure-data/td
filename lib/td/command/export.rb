
module TreasureData
module Command

  def table_export(op)
    org = nil
    from = nil
    to = nil
    s3_bucket = nil
    aws_access_key_id = nil
    aws_secret_access_key = nil
    file_format = "json.gz"

    op.on('-g', '--org ORGANIZATION', "export the data under this organization") {|s|
      org = s
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
    op.on('-k', '--aws-key-id KEY_ID', 'AWS access key id to export data (required)') {|s|
      aws_access_key_id = s
    }
    op.on('-s', '--aws-secret-key SECRET_KEY', 'AWS secret access key to export data (required)') {|s|
      aws_secret_access_key = s
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
    s3_opts['organization'] = org if org
    s3_opts['from'] = from.to_s if from
    s3_opts['to'] = to.to_s if to
    s3_opts['file_format'] = file_format
    s3_opts['bucket'] = s3_bucket
    s3_opts['access_key_id'] = aws_access_key_id
    s3_opts['secret_access_key'] = aws_secret_access_key

    job = client.export(db_name, table_name, "s3", s3_opts)

    $stderr.puts "Export job #{job.job_id} is queued."
    $stderr.puts "Use '#{$prog} job:show #{job.job_id}' to show the status."
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

