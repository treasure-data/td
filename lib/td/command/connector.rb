require 'td/command/common'
require 'td/command/job'
require 'td/connector_config_normalizer'
require 'json'
require 'uri'
require 'yaml'
require 'time'

module TreasureData
module Command

  def required(opt, value)
    if value.nil?
      raise ParameterConfigurationError, "#{opt} option required"
    end
  end

  def connector_guess(op)
    type = 's3'
    id = secret = source = nil
    out = 'config.yml'
    guess_plugins = {}

    op.on('--type[=TYPE]', "(obsoleted)") { |s| type = s }
    op.on('--access-id ID', "(obsoleted)") { |s| id = s }
    op.on('--access-secret SECRET', "(obsoleted)") { |s| secret = s }
    op.on('--source SOURCE', "(obsoleted)") { |s| source = s }
    op.on('-o', '--out FILE_NAME', "output file name for connector:preview") { |s| out = s }
    op.on('-g', '--guess NAME,NAME,...', 'specify list of guess plugins that users want to use') {|s|
      guess_plugins['guess_plugins'] = s.split(',')
    }

    config_file = op.cmd_parse
    if config_file
      config = prepare_bulkload_job_config(config_file)
      out ||= config_file
    else
      begin
        $stdout.puts 'Command line option is obsoleted. You should use configuration file.'
        required('--access-id', id)
        required('--access-secret', secret)
        required('--source', source)
        required('--out', out)
      rescue ParameterConfigurationError
        if id == nil && secret == nil && source == nil
          $stdout.puts op.to_s
          $stdout.puts ""
          raise ParameterConfigurationError, "path to configuration file is required"
        else
          raise
        end
      end

      uri = URI.parse(source)
      endpoint = uri.host
      path_components = uri.path.scan(/\/[^\/]*/)
      bucket = path_components.shift.sub(/\//, '')
      path_prefix = path_components.join.sub(/\//, '')

      config = {
        :type => type,
        :access_key_id => id,
        :secret_access_key => secret,
        :endpoint => endpoint,
        :bucket => bucket,
        :path_prefix => path_prefix,
      }
    end

    config = TreasureData::ConnectorConfigNormalizer.new(config).normalized_config
    config['exec'].merge!(guess_plugins)

    client = get_client
    job = client.bulk_load_guess(config: config)

    create_file_backup(out)
    if /\.json\z/ =~ out
      config_str = JSON.pretty_generate(job['config'])
    else
      config_str = config_to_yaml(job['config'])
    end
    File.open(out, 'w') do |f|
      f << config_str
    end

    $stdout.puts "Guessed configuration:"
    $stdout.puts
    $stdout.puts config_str
    $stdout.puts
    $stdout.puts "Created #{out} file."
    $stdout.puts "Use '#{$prog} " + Config.cl_options_string + "connector:preview #{out}' to see bulk load preview."
  end

  def connector_preview(op)
    set_render_format_option(op)
    config_file = op.cmd_parse
    config = prepare_bulkload_job_config(config_file)
    client = get_client()
    preview = client.bulk_load_preview(config: config)

    cols = preview['schema'].sort_by { |col|
      col['index']
    }
    fields = cols.map { |col| col['name'] + ':' + col['type'] }
    types = cols.map { |col| col['type'] }
    rows = preview['records'].map { |row|
      cols = {}
      row.each_with_index do |col, idx|
        cols[fields[idx]] = col.inspect
      end
      cols
    }

    $stdout.puts cmd_render_table(rows, :fields => fields, :render_format => op.render_format, :resize => false)

    $stdout.puts "Update #{config_file} and use '#{$prog} " + Config.cl_options_string + "connector:preview #{config_file}' to preview again."
    $stdout.puts "Use '#{$prog} " + Config.cl_options_string + "connector:issue #{config_file}' to run Server-side bulk load."
  end

  def connector_issue(op)
    database = table = nil
    time_column      = nil
    wait             = false
    auto_create      = false

    op.on('--database DB_NAME', "destination database") { |s| database = s }
    op.on('--table TABLE_NAME', "destination table") { |s| table = s }
    op.on('--time-column COLUMN_NAME', "data partitioning key") { |s| time_column = s }  # unnecessary but for backward compatibility
    op.on('-w', '--wait', 'wait for finishing the job', TrueClass) { |b| wait = b }
    op.on('--auto-create-table', "Create table and database if doesn't exist", TrueClass) { |b|
      auto_create = b
    }

    config_file = op.cmd_parse

    required('--database', database)
    required('--table', table)

    config = prepare_bulkload_job_config(config_file)
    (config['out'] ||= {})['time_column'] = time_column if time_column  # TODO will not work once embulk implements multi-job

    client = get_client()

    if auto_create
      create_database_and_table_if_not_exist(client, database, table)
    end

    job_id = client.bulk_load_issue(database, table, config: config)

    $stdout.puts "Job #{job_id} is queued."
    $stdout.puts "Use '#{$prog} " + Config.cl_options_string + "job:show #{job_id}' to show the status."

    if wait
      wait_connector_job(client, job_id)
    end
  end

  def connector_list(op)
    set_render_format_option(op)
    op.cmd_parse

    client = get_client()
    # TODO database and table is empty at present. Fix API or Client.
    keys = ['name', 'cron', 'timezone', 'delay', 'database', 'table']
    fields = keys.map { |e| e.capitalize.to_sym }
    rows = client.bulk_load_list().sort_by { |e|
      e['name']
    }.map { |e|
      Hash[fields.zip(e.values_at(*keys))]
    }

    $stdout.puts cmd_render_table(rows, :fields => fields, :render_format => op.render_format, resize: false)
  end

  def connector_create(op)
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

    config = prepare_bulkload_job_config(config_file)
    opts[:cron] = cron

    client = get_client()
    get_table(client, database, table)

    session = client.bulk_load_create(name, database, table, opts.merge(config: config))
    dump_connector_session(session)
  end

  def connector_show(op)
    name = op.cmd_parse

    client = get_client()
    session = client.bulk_load_show(name)
    dump_connector_session(session)
  end

  def connector_update(op)
    settings = {}

    op.on('-n', '--newname NAME', 'change the schedule\'s name', String) {|n|
      settings['name'] = n
    }
    op.on('-d', '--database DB_NAME', 'change the database', String) {|s|
      settings['database'] = s
    }
    op.on('-t', '--table TABLE_NAME', 'change the table', String) {|s|
      settings['table'] = s
    }
    op.on('-s', '--schedule [CRON]', 'change the schedule or leave blank to remove the schedule', String) {|s|
      settings['cron'] = s || ''
    }
    op.on('-z', '--timezone TZ', "name of the timezone.",
                                 "  Only extended timezones like 'Asia/Tokyo', 'America/Los_Angeles' are supported,",
                                 "  (no 'PST', 'PDT', etc...).",
                                 "  When a timezone is specified, the cron schedule is referred to that timezone.",
                                 "  Otherwise, the cron schedule is referred to the UTC timezone.",
                                 "  E.g. cron schedule '0 12 * * *' will execute daily at 5 AM without timezone option",
                                 "  and at 12PM with the -t / --timezone 'America/Los_Angeles' timezone option", String) {|s|
      settings['timezone'] = s
    }
    op.on('-D', '--delay SECONDS', 'change the delay time of the schedule', Integer) {|i|
      settings['delay'] = i
    }
    op.on('-T', '--time-column COLUMN_NAME', 'change the name of the time column', String) {|s|
      settings['time_column'] = s
    }
    op.on('-c', '--config CONFIG_FILE', 'update the connector configuration', String) {|s|
      settings['config'] = s
    }
    op.on('--config-diff CONFIG_DIFF_FILE', "update the connector config_diff", String) { |s| settings['config_diff'] = s }

    name, config_file = op.cmd_parse
    settings['config'] = config_file if config_file
    op.cmd_usage 'nothing to update' if settings.empty?
    settings['config'] = prepare_bulkload_job_config(settings['config']) if settings.key?('config')
    settings['config_diff'] = prepare_bulkload_job_config(settings['config_diff']) if settings.key?('config_diff')
    client = get_client()
    session = client.bulk_load_update(name, settings)
    dump_connector_session(session)
  end

  def connector_delete(op)
    name = op.cmd_parse

    client = get_client()
    session = client.bulk_load_delete(name)
    $stdout.puts 'Deleted session'
    $stdout.puts '--'
    dump_connector_session(session)
  end

  def connector_history(op)
    set_render_format_option(op)
    name = op.cmd_parse

    fields = [:JobID, :Status, :Records, :Database, :Table, :Priority, :Started, :Duration]
    client = get_client()
    rows = client.bulk_load_history(name).map { |e|
      time_property = if e['start_at']
        {
          :Started => Time.at(e['start_at']),
          :Duration => (e['end_at'].nil? ? Time.now.to_i : e['end_at']) - e['start_at'],
        }
      else
        {:Started => '', :Duration => ''}
      end

      {
        :JobID => e['job_id'],
        :Status => e['status'],
        :Records => e['records'],
        # TODO: td-client-ruby should retuan only name
        :Database => e['database'] ? e['database']['name'] : '',
        :Table    => e['table']    ? e['table']['name']    : '',
        :Priority => e['priority'],
      }.merge(time_property)
    }
    $stdout.puts cmd_render_table(rows, :fields => fields, :render_format => op.render_format)
  end

  def connector_run(op)
    wait = false
    op.on('-w', '--wait', 'wait for finishing the job', TrueClass) { |b| wait = b }

    name, scheduled_time = op.cmd_parse
    time = if scheduled_time
      Time.parse(scheduled_time).to_i
    else
      current_time.to_i
    end

    client = get_client()
    job_id = client.bulk_load_run(name, time)

    $stdout.puts "Job #{job_id} is queued."
    $stdout.puts "Use '#{$prog} " + Config.cl_options_string + "job:show #{job_id}' to show the status."

    if wait
      wait_connector_job(client, job_id)
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

  def config_to_yaml(config)
    config_str = ''
    begin
      require 'td/compact_format_yamler'
      config_str = TreasureData::CompactFormatYamler.dump(config)
    rescue
      # NOTE fail back
      config_str = YAML.dump(config)
    end
    config_str
  end


  def prepare_bulkload_job_config(config_file)
    config = prepare_bulkload_job_config_diff(config_file)
    TreasureData::ConnectorConfigNormalizer.new(config).normalized_config
  end

  def prepare_bulkload_job_config_diff(config_file)
    unless File.exist?(config_file)
      raise ParameterConfigurationError, "configuration file: #{config_file} not found"
    end
    config_str = File.read(config_file)

    config = nil
    begin
      if file_type(config_str) == :yaml
        config_str = JSON.pretty_generate(YAML.load(config_str))
      end
      config = JSON.load(config_str)
    rescue => e
      raise ParameterConfigurationError, "configuration file: #{config_file} #{e.message}"
    end
    config
  end

  def dump_connector_session(session)
    $stdout.puts "Name     : #{session["name"]}"
    $stdout.puts "Cron     : #{session["cron"]}"
    $stdout.puts "Timezone : #{session["timezone"]}"
    $stdout.puts "Delay    : #{session["delay"]}"
    $stdout.puts "Database : #{session["database"]}"
    $stdout.puts "Table    : #{session["table"]}"
    $stdout.puts "Config"
    $stdout.puts YAML.dump(session["config"])
    $stdout.puts
    $stdout.puts "Config Diff"
    $stdout.puts YAML.dump(session["config_diff"])
  end

  def wait_connector_job(client, job_id)
    job = client.job(job_id)
    wait_job(job, true)
    $stdout.puts "Status     : #{job.status}"
  end

  def current_time
    Time.now
  end
end
end
