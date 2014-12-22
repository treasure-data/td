require 'td/command/common'
require 'uri'

module TreasureData
module Command

  def required(opt, value)
    if value.nil?
      raise ParameterConfigurationError, "#{opt} option required when config file is not specified"
    end
  end

  def bulk_load_guess(op)
    type = 's3_file'
    id = secret = source = database = table = nil
    out = 'td-bulkload.yml'

    op.on('--type[=TYPE]', "Server-side bulk_load type; only 's3_file' is supported") { |s| type = s }
    op.on('--access-id ID', "access ID (S3 access key id for type: s3_file)") { |s| id = s }
    op.on('--access-secret SECRET', "access secret (S3 secret access key for type: s3_file)") { |s| secret = s }
    op.on('--source SOURCE', "resource(s) URI to be imported (e.g. https://s3-us-west-1.amazonaws.com/bucketname/path/prefix/to/import/)") { |s| source = s }
    op.on('--database DB_NAME', "destination database") { |s| database = s }
    op.on('--table TABLE_NAME', "destination table") { |s| table = s }
    op.on('--out', "configuration file") { |s| out = s }

    config = op.cmd_parse
    out ||= config

    if config
      job = API::BulkLoad::Job.from_json(File.read(config))
    else
      required('--access-id', id)
      required('--access-secret', secret)
      required('--source', source)
      required('--database', database)
      required('--table', table)
      required('--out', out)

      uri = URI.parse(source)
      endpoint = uri.host
      path_components = uri.path.scan(/\/[^\/]*/)
      bucket = path_components.shift
      path = path_components.join

      job = API::BulkLoad::Job.from_hash(
        :config => {
          :type => type,
          :access_key_id => id,
          :secret_access_key => secret,
          :endpoint => endpoint,
          :bucket => bucket,
          :paths => [
            path
          ]
        },
        :database => database,
        :table => table
      ).validate
    end

    client = get_client
    job = client.bulk_load_guess(job)

    create_bulkload_job_file_backup(out)
    File.open(out, 'w') do |f|
      f << JSON.pretty_generate(job.to_h)
    end

    puts "Created #{out} file."
    puts "Use '#{$prog} " + Config.cl_options_string + "bulk_load:preview --load-config #{out} to preview Server-side bulk load."
  end

  def bulk_load_preview(op)
    job = prepare_bulkload_job_config(op)
    client = get_client()
    preview = client.bulk_load_preview(job)

    # TODO: pretty printing
    puts preview
  end

  def bulk_load_issue(op)
    job = prepare_bulkload_job_config(op)
    client = get_client()
    job_id = client.bulk_load_issue(job)

    puts "Job #{job_id} is queued."
    puts "Use '#{$prog} " + Config.cl_options_string + "job:show #{job_id}' to show the status."
  end

private

  def prepare_bulkload_job_config(op)
    config = op.cmd_parse
    unless File.exist?(config)
      raise ParameterConfigurationError, "configuration file: #{config} not found"
    end
    API::BulkLoad::Job.from_json(File.read(config))
  end

  def create_bulkload_job_file_backup(out)
    return unless File.exist?(out)
    0.upto(100) do |idx|
      backup = "#{out}.#{idx}"
      unless File.exist?(backup)
        FileUtils.mv(out, backup)
        return
      end
    end
    raise "backup file creation failed"
  end

end
end

