require 'td/command/common'
require 'uri'
require 'yaml'

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
    op.on('--out FILE_NAME', "configuration file") { |s| out = s }

    config = op.cmd_parse
    if config
      job = API::BulkLoad::Job.from_json(File.read(config))
      out ||= config
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
      bucket = path_components.shift.sub(/\//, '')
      path = path_components.join.sub(/\//, '')

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

    # TODO: API should keep database/table in config
    job['database'] ||= database
    job['table'] ||= table

    create_bulkload_job_file_backup(out)
    if /\.json\z/ =~ out
      config_str = JSON.pretty_generate(job.to_h)
    else
      config_str = YAML.dump(job.to_h)
    end
    File.open(out, 'w') do |f|
      f << config_str
    end

    puts "Created #{out} file."
    puts "Use '#{$prog} " + Config.cl_options_string + "bulk_load:preview #{out}' to see bulk load preview."
  end

  def bulk_load_preview(op)
    config_file = op.cmd_parse
    job = prepare_bulkload_job_config(config_file)
    client = get_client()
    preview = client.bulk_load_preview(job)

    # TODO: pretty printing
    require 'pp'
    pp preview

    puts "Update #{config_file} and use '#{$prog} " + Config.cl_options_string + "bulk_load:preview #{config_file}' to preview again."
    puts "Use '#{$prog} " + Config.cl_options_string + "bulk_load:issue #{config_file}' to run Server-side bulk load."
  end

  def bulk_load_issue(op)
    config_file = op.cmd_parse
    job = prepare_bulkload_job_config(config_file)
    client = get_client()
    job_id = client.bulk_load_issue(job)

    puts "Job #{job_id} is queued."
    puts "Use '#{$prog} " + Config.cl_options_string + "job:show #{job_id}' to show the status."
  end

private

  def file_type(str)
    begin
      YAML.load(str)
      return :yaml
    rescue
    end
    begin
      JSON.parse(str)
      return :json
    rescue
    end
    nil
  end

  def prepare_bulkload_job_config(config_file)
    unless File.exist?(config_file)
      raise ParameterConfigurationError, "configuration file: #{config_file} not found"
    end
    config_str = File.read(config_file)
    if file_type(config_str) == :yaml
      config_str = JSON.pretty_generate(YAML.load(config_str))
    end
    API::BulkLoad::Job.from_json(config_str)
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

