require 'td/command/common'
require 'td/command/job'
require 'json'
require 'uri'
require 'yaml'

module TreasureData
module Command

  def required(opt, value)
    if value.nil?
      raise ParameterConfigurationError, "#{opt} option required"
    end
  end

  def bulk_load_guess(op)
    type = 's3'
    id = secret = source = nil
    out = 'td-bulkload.yml'

    op.on('--type[=TYPE]', "Server-side bulk_load type; only 's3' is supported") { |s| type = s }
    op.on('--access-id ID', "access ID (S3 access key id for type: s3)") { |s| id = s }
    op.on('--access-secret SECRET', "access secret (S3 secret access key for type: s3)") { |s| secret = s }
    op.on('--source SOURCE', "resource(s) URI to be imported (e.g. https://s3-us-west-1.amazonaws.com/bucketname/path/prefix/to/import/)") { |s| source = s }
    op.on('--out FILE_NAME', "configuration file") { |s| out = s }

    config = op.cmd_parse
    if config
      job = prepare_bulkload_job_config(config)
      out ||= config
    else
      required('--access-id', id)
      required('--access-secret', secret)
      required('--source', source)
      required('--out', out)

      uri = URI.parse(source)
      endpoint = uri.host
      path_components = uri.path.scan(/\/[^\/]*/)
      bucket = path_components.shift.sub(/\//, '')
      path_prefix = path_components.join.sub(/\//, '')

      job = API::BulkLoad::BulkLoad.from_hash(
        :config => {
          :type => type,
          :access_key_id => id,
          :secret_access_key => secret,
          :endpoint => endpoint,
          :bucket => bucket,
          :path_prefix => path_prefix,
        }
      ).validate
    end

    client = get_client
    job = client.bulk_load_guess(job)

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
    set_render_format_option(op)
    config_file = op.cmd_parse
    job = prepare_bulkload_job_config(config_file)
    client = get_client()
    preview = client.bulk_load_preview(job)

    cols = preview.schema.sort_by { |col|
      col['index']
    }
    fields = cols.map { |col| col['name'] + ':' + col['type'] }
    types = cols.map { |col| col['type'] }
    rows = preview.records.map { |row|
      cols = {}
      row.each_with_index do |col, idx|
        value = case types[idx]
          when 'timestamp'
            Time.at(col['epochSecond'])
          when 'int', 'long', 'double'
            col
          else
            col.to_s.dump
          end
        cols[fields[idx]] = value
      end
      cols
    } 

    puts cmd_render_table(rows, :fields => fields, :render_format => op.render_format)

    puts "Update #{config_file} and use '#{$prog} " + Config.cl_options_string + "bulk_load:preview #{config_file}' to preview again."
    puts "Use '#{$prog} " + Config.cl_options_string + "bulk_load:issue #{config_file}' to run Server-side bulk load."
  end

  def bulk_load_issue(op)
    database = table = nil
    time_column = nil
    wait = exclude = false
    op.on('--database DB_NAME', "destination database") { |s| database = s }
    op.on('--table TABLE_NAME', "destination table") { |s| table = s }
    op.on('--time-column COLUMN_NAME', "data partitioning key") { |s| time_column = s }
    op.on('-w', '--wait', 'wait for finishing the job', TrueClass) { |b| wait = b }
    op.on('-x', '--exclude', 'do not automatically retrieve the job result', TrueClass) { |b| exclude = b }

    config_file = op.cmd_parse

    required('--database', database)
    required('--table', table)

    job = prepare_bulkload_job_config(config_file)
    job['time_column'] = time_column if time_column

    client = get_client()
    job_id = client.bulk_load_issue(database, table, job)

    puts "Job #{job_id} is queued."
    puts "Use '#{$prog} " + Config.cl_options_string + "job:show #{job_id}' to show the status."

    if wait
      wait_bulk_load_job(client, job_id, exclude)
    end
  end

  def bulk_load_list(op)
    set_render_format_option(op)
    op.cmd_parse

    client = get_client()
    # TODO database and table is empty at present. Fix API or Client.
    keys = ['name', 'cron', 'timezone', 'delay', 'database', 'table', 'config']
    fields = keys.map { |e| e.capitalize.to_sym }
    rows = client.bulk_load_list().sort_by { |e|
      e['name']
    }.map { |e|
      Hash[fields.zip(e.to_h.values_at(*keys))]
    }

    puts cmd_render_table(rows, :fields => fields, :render_format => op.render_format)
  end

  def bulk_load_create(op)
    # TODO it's a must parameter at this moment but API should be fixed
    opts = {:timezone => 'UTC'}
    op.on('--time-column COLUMN_NAME', "data partitioning key") {|s|
      opts[:time_column] = s
    }
    op.on('-t', '--timezone TZ', "name of the timezone.",
                                 "  Only extended timezones like 'Asia/Tokyo', 'America/Los_Angeles' are supported,",
                                 "  (no 'PST', 'PDT', etc...).",
                                 "  When a timezone is specified, the cron schedule is referred to that timezone.",
                                 "  Otherwise, the cron schedule is referred to the UTC timezone.",
                                 "  E.g. cron schedule '0 12 * * *' will execute daily at 5 AM without timezone option",
                                 "  and at 12PM with the -t / --timezone 'America/Los_Angeles' timezone option") {|s|
      opts[:timezone] = s
    }
    op.on('-D', '--delay SECONDS', 'delay time of the schedule', Integer) {|i|
      opts[:delay] = i
    }

    name, cron, database, table, config_file = op.cmd_parse

    job = prepare_bulkload_job_config(config_file)
    opts[:cron] = cron

    client = get_client()
    get_table(client, database, table)

    session = client.bulk_load_create(name, database, table, job, opts)
    dump_bulk_load_session(session)
  end

  def bulk_load_show(op)
    name = op.cmd_parse

    client = get_client()
    session = client.bulk_load_show(name)
    dump_bulk_load_session(session)
  end

  def bulk_load_update(op)
    name, config_file = op.cmd_parse

    job = prepare_bulkload_job_config(config_file)

    client = get_client()
    session = client.bulk_load_update(name, job)
    dump_bulk_load_session(session)
  end

  def bulk_load_delete(op)
    name = op.cmd_parse

    client = get_client()
    session = client.bulk_load_delete(name)
    puts 'Deleted session'
    puts '--'
    dump_bulk_load_session(session)
  end

  def bulk_load_history(op)
    set_render_format_option(op)
    name = op.cmd_parse

    fields = [:JobID, :Status, :Records, :Database, :Table, :Priority, :Started, :Duration]
    client = get_client()
    rows = client.bulk_load_history(name).map { |e|
      {
        :JobID => e.job_id,
        :Status => e.status,
        :Records => e.records,
        # TODO: td-client-ruby should retuan only name
        :Database => e.database['name'],
        :Table => e.table['name'],
        :Priority => e.priority,
        :Started => Time.at(e.start_at),
        :Duration => (e.end_at.nil? ? Time.now.to_i : e.end_at) - e.start_at,
      }
    }
    puts cmd_render_table(rows, :fields => fields, :render_format => op.render_format)
  end

  def bulk_load_run(op)
    wait = exclude = false
    op.on('-w', '--wait', 'wait for finishing the job', TrueClass) { |b| wait = b }
    op.on('-x', '--exclude', 'do not automatically retrieve the job result', TrueClass) { |b| exclude = b }

    name, scheduled_time = op.cmd_parse

    client = get_client()
    job_id = client.bulk_load_run(name)
    puts "Job #{job_id} is queued."
    puts "Use '#{$prog} " + Config.cl_options_string + "job:show #{job_id}' to show the status."

    if wait
      wait_bulk_load_job(client, job_id, exclude)
    end
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
    API::BulkLoad::BulkLoad.from_json(config_str)
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

  def dump_bulk_load_session(session)
    puts "Name     : #{session.name}"
    puts "Cron     : #{session.cron}"
    puts "Timezone : #{session.timezone}"
    puts "Delay    : #{session.delay}"
    puts "Database : #{session.database}"
    puts "Table    : #{session.table}"
    puts "Config"
    puts YAML.dump(session.config.to_h)
  end

  def wait_bulk_load_job(client, job_id, exclude)
    job = client.job(job_id)
    wait_job(job, true)
    puts "Status     : #{job.status}"
  end

end
end
