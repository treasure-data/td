require 'td/updater'
require 'time'
require 'yaml'

module TreasureData
module Command
  include TreasureData::Updater

  JAVA_COMMAND = "java"
  JAVA_MAIN_CLASS = "com.treasure_data.td_import.BulkImportCommand"
  JVM_OPTS = ["-Xmx1024m"] # TODO

  def import_list(op)
    require 'td/command/bulk_import'
    bulk_import_list(op)
  end

  def import_show(op)
    require 'td/command/bulk_import'
    bulk_import_show(op)
  end

  def import_create(op)
    require 'td/command/bulk_import'
    bulk_import_create(op)
  end

  def import_jar_version(op)
    op.cmd_parse
    version = find_version_file
    $stdout.puts "td-import-java #{File.open(version, 'r').read}"
  end

  def import_jar_update(op)
    op.cmd_parse
    check_n_update_jar(false)
  end

  def import_prepare(op)
    import_by_java(op)
  end

  def import_upload(op)
    import_by_java(op)
  end

  def import_auto(op)
    import_by_java(op)
  end

  def import_perform(op)
    require 'td/command/bulk_import'
    bulk_import_perform(op)
  end

  def import_error_records(op)
    require 'td/command/bulk_import'
    bulk_import_error_records(op)
  end

  def import_commit(op)
    require 'td/command/bulk_import'
    bulk_import_commit(op)
  end

  def import_delete(op)
    require 'td/command/bulk_import'
    bulk_import_delete(op)
  end

  def import_freeze(op)
    require 'td/command/bulk_import'
    bulk_import_freeze(op)
  end

  def import_unfreeze(op)
    require 'td/command/bulk_import'
    bulk_import_unfreeze(op)
  end

  def import_config(op)
    out = 'td-bulkload.yml'
    options = {
      'format' => 'csv'
    }
    not_migrate_options = []
    op.on('-o', '--out FILE_NAME', "output file name for connector:guess") { |s| out = s }
    op.on('-f', '--format FORMAT', "source file format [csv, tsv, mysql]; default=csv") { |s| options['format'] = s }

    op.on('--db-url URL',           "Database Connection URL") { |s| options['db_url']      = s }
    op.on('--db-user NAME',         "user name for database")  { |s| options['db_user']     = s }
    op.on('--db-password PASSWORD', "password for database")   { |s| options['db_password'] = s }
    %w(--columns --column-header --time-column --time-format).each do |not_migrate_option|
      opt_arg_name = not_migrate_option.gsub('--', '').upcase
      op.on("#{not_migrate_option} #{opt_arg_name}", 'not supported') { |s| not_migrate_options << not_migrate_option }
    end

    arg = op.cmd_parse

    unless %w(mysql csv tsv).include?(options['format'])
      raise ParameterConfigurationError, "#{options['format']} is unknown format. Support format is csv, tsv and mysql."
    end

    unless not_migrate_options.empty?
      be = not_migrate_options.size == 1 ? 'is' : 'are'
      $stderr.puts "`#{not_migrate_options.join(', ')}` #{be} not migrate. Please, edit config file after execute guess commands."
    end

    $stdout.puts "Generating #{out}..."

    config = generate_seed_confing(options['format'], arg, options)
    config_str = YAML.dump(config)


    create_file_backup(out)
    File.open(out, 'w') {|f| f << config_str }

    if config['out']['type'] == 'td'
      show_message_for_td_output_plugin(out)
    else
      show_message_for_td_data_connector(out)
    end
  end

  #
  # Module private methods - don't map to import:* commands
  #

  private
  def import_by_java(op)
    subcmd = op.name.split(/:/)[1]
    begin
      check_n_update_jar(true)
    rescue UpdateError => e
      if op.cmd_requires_connectivity
        raise e
      else
        $stdout.puts "Warning: JAR update skipped for connectivity issues"
      end
    end

    # check java runtime exists or not
    check_java

    # show help?
    show_help = ARGV.size == 0 || (ARGV.size == 1 || ARGV[0] =~ /^import:/)

    # configure java command-line arguments
    timeout = nil
    java_args = []
    java_args.concat build_sysprops
    java_args.concat ["-cp", find_td_import_jar]
    java_args << JAVA_MAIN_CLASS
    java_args << subcmd
    if show_help
      java_args << "--help"
    else
      0.upto(ARGV.length - 1) do |idx|
        if ARGV[idx] == '--bulk-import-timeout'
          timeout = ARGV[idx + 1]
          if timeout.nil?
            raise ArgumentError, 'timeout not given'
          end
          timeout = Integer(timeout)
          ARGV.slice!(idx, 2)
        end
      end
      java_args.concat ARGV
    end
    cmd = [JAVA_COMMAND] + JVM_OPTS + java_args

    CommandExecutor.new(cmd, timeout).execute
  end

  class CommandExecutor
    def initialize(cmd, timeout)
      @cmd, @timeout = cmd, timeout
    end

    def execute
      status = execute_command
      if status.exitstatus != 0
        raise BulkImportExecutionError,
              "Bulk Import returned error #{status.exitstatus}. Please check the 'td-bulk-import.log' logfile for details."
      end
      status
    end

  private

    def execute_command
      if @timeout
        require 'timeout'
        pid = nil
        begin
          Timeout.timeout(@timeout) do
            pid = Process.spawn(*@cmd)
            waitpid(pid)
          end
        rescue Timeout::Error
          if pid
            # NOTE last check has not been completed the process during sleep
            if Process.waitpid(pid, Process::WNOHANG)
              return $?
            end

            require 'rbconfig'
            # win32 ruby does not support QUIT and TERM
            if RbConfig::CONFIG['host_os'] !~ /mswin|mingw|cygwin/
              Process.kill('QUIT', pid)
              Process.kill('TERM', pid)
            else
              # just kill without thread dump on win32 platforms
              Process.kill('KILL', pid)
            end
          end
          raise BulkImportExecutionError, "Bulk Import execution timed out: #{@timeout} [sec]"
        end
      else
        system(*@cmd)
        return $?
      end
    end

    def waitpid(pid)
      # NOTE Use nonblocking mode, because Process.waitpid is block other thread at Windows.
      loop do
        if Process.waitpid(pid, Process::WNOHANG)
          return $?
        end

        sleep 1
      end
    end
  end

  def check_java
    if RbConfig::CONFIG["target_os"].downcase =~ /mswin(?!ce)|mingw|cygwin|bccwin/ # windows
      cmd = "#{JAVA_COMMAND} -version > NUL 2>&1"
    else # others
      cmd = "#{JAVA_COMMAND} -version > /dev/null 2>&1"
    end

    system(cmd)

    unless $?.success?
      $stderr.puts "Java is not installed. 'td import' command requires Java (version 1.6 or later)."
      $stderr.puts "Alternatively, you can use the 'bulk_import' commands."
      $stderr.puts "Since they are implemented in Ruby, they perform significantly slower."
      exit 1
    end
  end

  private
  def build_sysprops
    sysprops = []

    # set apiserver
    set_sysprops_endpoint(sysprops)
    # set http_proxy
    set_sysprops_http_proxy(sysprops)

    # set configuration file for logging
    conf_file = find_logging_property
    unless conf_file.empty?
      sysprops << "-Djava.util.logging.config.file=#{conf_file}"
    end

    # set API key
    sysprops << "-Dtd.api.key=#{TreasureData::Config.apikey}"

    sysprops
  end

  private
  def set_sysprops_endpoint(sysprops)
    # optional, if not provided a default is used from the ruby client library
    begin
      endpoint = Config.endpoint
    rescue ConfigNotFoundError => e
      # rescue the ConfigNotFoundError exception which originates when
      #   the config file is not found or an endpoint is not provided
      #   either through -e / --endpoint or TD_API_SERVER environment
      #   variable.
    end

    if endpoint
      require 'uri'

      uri = URI.parse(endpoint)

      case uri.scheme
      when 'http', 'https'
        host = uri.host
        port = uri.port
        # NOTE: Config.secure option is ignored in favor
        # of defining ssl based on the URL scheme
        ssl = (uri.scheme == 'https')
      else
        # uri scheme is not set if the endpoint is
        #   like 'api.treasuredata.com:80'. In that case the
        #   uri object does not contain any useful info.
        if uri.port # invalid URI
          raise "Invalid endpoint: #{endpoint}"
        end

        # generic URI
        host, port = endpoint.split(':', 2)
        port = port.to_i
        # Config.secure = false is the --insecure option was used
        if Config.secure
          port = 443 if port == 0
          ssl = true
        else
          port = 80 if port == 0
          ssl = false
        end
      end

      sysprops << "-Dtd.api.server.scheme=#{ssl ? 'https' : 'http'}://"
      sysprops << "-Dtd.api.server.host=#{host}"
      sysprops << "-Dtd.api.server.port=#{port}"
    end
  end

  private
  def set_sysprops_http_proxy(sysprops)
    uri_string = ENV['HTTP_PROXY']
    return unless uri_string

    require 'uri'
    uri = URI.parse(uri_string)
    proxy_host = nil
    proxy_port = nil

    case uri.scheme
    when 'http'
      if uri.host
        proxy_host = uri.host             # host is required
        proxy_port = uri.port if uri.port # default value of uri.port is 80.
      end
    when 'https'
      raise ParameterConfigurationError,
            "HTTP proxy URL must use 'http' protocol. Example format: 'http://localhost:3128'."
    else
      proxy_host, proxy_port = uri_string.split(':', 2)
      proxy_port = (proxy_port ? proxy_port.to_i : 80)
    end

    sysprops << "-Dhttp.proxyHost=#{proxy_host}" if proxy_host
    sysprops << "-Dhttps.proxyHost=#{ proxy_host}" if proxy_host
    sysprops << "-Dhttp.proxyPort=#{proxy_port}" if proxy_port
    sysprops << "-Dhttps.proxyPort=#{ proxy_port}" if proxy_port
  end

  #
  # Helpers
  #

  # find logging.properties file, first in the jarfile_dest_path, then in the
  #   installed_path
  def find_logging_property
    installed_path = File.join(File.expand_path('../../..', File.dirname(__FILE__)), 'java')
    config = Command.find_files("logging.properties", [Updater.jarfile_dest_path, installed_path])
    if config.empty?
      $stdout.puts "Cannot find 'logging.properties' file in '#{Updater.jarfile_dest_path}' or " +
           "'#{installed_path}'." unless ENV['TD_TOOLBELT_DEBUG'].nil?
      []
    else
      config.first
    end
  end

  def find_td_import_jar
    jar = Command.find_files('td-import.jar', [Updater.jarfile_dest_path])
    if jar.empty?
      $stderr.puts "Cannot find td-import.jar in '#{Updater.jarfile_dest_path}'."
      exit 10
    end
    jar.first
  end

  def find_version_file
    version = Command.find_files('VERSION', [Updater.jarfile_dest_path])
    if version.empty?
      $stderr.puts "Cannot find VERSION file in '#{Updater.jarfile_dest_path}'."
      exit 10
    end
    version.first
  end

  def generate_seed_confing(format, arg, options)
    case format
    when 'csv', 'tsv'
      if arg =~ /^s3:/
        generate_s3_config(format, arg)
      else
        generate_csv_config(format, arg)
      end
    when 'mysql'
      arg = arg[1] unless arg.class == String
      generate_mysql_config(arg, options)
    else
      # NOOP
    end
  end

  def generate_s3_config(format, arg)
    puts_with_indent('Using S3 input')
    puts_with_indent('Using CSV parser plugin')
    puts_with_indent('Using Treasure Data data connector')

    match = Regexp.new("^s3://(.*):(.*)@/([^/]*)/(.*)").match(arg)

    {
      'in' => {
        'type' => 's3',
        'access_key_id'     => match[1],
        'secret_access_key' => match[2],
        'bucket'            => match[3],
        'path_prefix'       => normalize_path_prefix(match[4])
      },
      'out' => {'mode' => 'append'}
    }
  end

  def generate_csv_config(format, arg)
    puts_with_indent('Using local file input')
    puts_with_indent('Using CSV parser plugin')
    puts_with_indent('Using Treasure Data output')

    {
      'in' => {
        'type'        => 'file',
        'path_prefix' => normalize_path_prefix(arg),
        'decorders'   => [{'type' => 'gzip'}],
      },
      'out' => td_output_config,
    }
  end

  def td_output_config
    {
      'type' => 'td',
      'endpoint' => Config.cl_endpoint || Config.endpoint,
      'apikey' => Config.cl_apikey || Config.apikey,
      'database' => '',
      'table' => '',
    }
  end

  def normalize_path_prefix(path)
    path.gsub(/\*.*/, '')
  end

  def generate_mysql_config(arg, options)
    puts_with_indent('Using MySQL input')
    puts_with_indent('Using MySQL parser plugin')
    puts_with_indent('Using Treasure Data output')

    mysql_url_regexp = Regexp.new("[jdbc:]*mysql://(?<host>[^:/]*)[:]*(?<port>[^/]*)/(?<db_name>.*)")

    config = if (match = mysql_url_regexp.match(options['db_url']))
      {
        'host'     => match['host'],
        'port'     => match['port'] == '' ? 3306 : match['port'].to_i,
        'database' => match['db_name'],
      }
    else
      {
        'host'     => '',
        'port'     => 3306,
        'database' => '',
      }
    end

    {
      'in' => config.merge(
        'type'     => 'mysql',
        'user'     => options['db_user'],
        'password' => options['db_password'],
        'table'    => arg,
        'select'   => '*',
      ),
      'out' => td_output_config,
    }
  end

    def show_message_for_td_output_plugin(out)
      $stdout.puts 'Done. Please use embulk to load the files.'
      $stdout.puts 'Next steps:'
      $stdout.puts
      puts_with_indent '# install embulk'
      puts_with_indent "$ embulk gem install embulk-output-td"
      puts_with_indent '$ embulk guess seed.yml -o config.yml'
      puts_with_indent '$ embulk preview config.yml'
      puts_with_indent '$ embulk run config.yml'
    end

    def show_message_for_td_data_connector(out)
      $stdout.puts 'Done. Please use connector:guess and connector:run to load the files.'
      $stdout.puts 'Next steps:'
      puts_with_indent "$ td connector:guess #{out} -o config.yml"
      puts_with_indent '$ td connector:preview config.yml'
      puts_with_indent '$ td connector:run config.yml'
    end

end
end
