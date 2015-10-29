require 'td/command/common'
require 'td/command/job'
require 'td/connector_config_normalizer'
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
    $stdout.puts "Use '#{$prog} " + Config.cl_options_string + "connector:issue #{config_file}' to run data connector."
  end

  def connector_issue(op)
    wait = exclude = false
    auto_create    = false
    config_option  = {}

    on_with_obsolute_and_overwrite_config_warning(op, '--database DB_NAME') { |s| config_option['database'] = s }
    on_with_obsolute_and_overwrite_config_warning(op, '--table TABLE_NAME') { |s| config_option['table'] = s }
    on_with_obsolute_and_overwrite_config_warning(op, '--time-column COLUMN_NAME') { |s| config_option['time_column'] = s }

    op.on('-w', '--wait', 'wait for finishing the job', TrueClass) { |b| wait = b }
    op.on('-x', '--exclude', 'do not automatically retrieve the job result', TrueClass) { |b| exclude = b }
    op.on('--auto-create-table', "Create table and database if doesn't exist", TrueClass) { |b|
      auto_create = b
    }

    config_file = op.cmd_parse

    config = prepare_bulkload_job_config(config_file)
    overwrite_out_config(config, config_option)

    client = get_client()

    required('database', config['out']['database'])
    required('table', config['out']['table'])

    # TODO need fix if embulk support multi-job
    if auto_create
      create_database_and_table_if_not_exist(client, config['out']['database'], config['out']['table'])
    end

    job_id = client.bulk_load_issue(config['out']['database'], config['out']['table'], config: config)

    $stdout.puts "Job #{job_id} is queued."
    $stdout.puts "Use '#{$prog} " + Config.cl_options_string + "job:show #{job_id}' to show the status."

    if wait
      wait_connector_job(client, job_id, exclude)
    end
  end

  def on_with_obsolute_and_overwrite_config_warning(op, *args, &block)
    options = args.each_with_object([]) do |arg, o|
      arg.split("\s").each do |word|
        o << word if word =~ /\A-/
      end
    end

    op.on(*args, '(obsoleted)') do |s|
      $stderr.puts "#{options.join(',')} #{options.size > 1 ? 'are' : 'is'} obsolete option. You should write to configuration file. Even if you wrote in the configuration file, #{s} is used."
      block.call(s)
    end
  end

  def overwrite_out_config(config, out_args)
    # TODO will not work once embulk implements multi-job
    config['out'] ||= {}

    out_args.each do |key, value|
      config['out'][key] = value if value
    end
  end

  def connector_list(op)
    set_render_format_option(op)
    op.cmd_parse

    client = get_client()
    # TODO database and table is empty at present. Fix API or Client.
    keys = ['name', 'cron', 'timezone', 'delay', 'database', 'table', 'config']
    fields = keys.map { |e| e.capitalize.to_sym }
    rows = client.bulk_load_list().sort_by { |e|
      e['name']
    }.map { |e|
      Hash[fields.zip(e.values_at(*keys))]
    }

    $stdout.puts cmd_render_table(rows, :fields => fields, :render_format => op.render_format)
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
    name, config_file = op.cmd_parse

    config = prepare_bulkload_job_config(config_file)

    client = get_client()
    session = client.bulk_load_update(name, config: config)
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
      {
        :JobID => e['job_id'],
        :Status => e['status'],
        :Records => e['records'],
        # TODO: td-client-ruby should retuan only name
        :Database => e['database']['name'],
        :Table => e['table']['name'],
        :Priority => e['priority'],
        :Started => Time.at(e['start_at']),
        :Duration => (e['end_at'].nil? ? Time.now.to_i : e['end_at']) - e['start_at'],
      }
    }
    $stdout.puts cmd_render_table(rows, :fields => fields, :render_format => op.render_format)
  end

  def connector_run(op)
    wait = exclude = false
    op.on('-w', '--wait', 'wait for finishing the job', TrueClass) { |b| wait = b }
    op.on('-x', '--exclude', 'do not automatically retrieve the job result', TrueClass) { |b| exclude = b }

    name, scheduled_time = op.cmd_parse

    client = get_client()
    job_id = client.bulk_load_run(name)
    $stdout.puts "Job #{job_id} is queued."
    $stdout.puts "Use '#{$prog} " + Config.cl_options_string + "job:show #{job_id}' to show the status."

    if wait
      wait_connector_job(client, job_id, exclude)
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

    TreasureData::ConnectorConfigNormalizer.new(config).normalized_config
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
  end

  def wait_connector_job(client, job_id, exclude)
    job = client.job(job_id)
    wait_job(job, true)
    $stdout.puts "Status     : #{job.status}"
  end

end
end
