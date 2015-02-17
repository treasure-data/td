require 'td/updater'
require 'time'

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
    puts "td-import-java #{File.open(version, 'r').read}"
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
        puts "Warning: JAR update skipped for connectivity issues"
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
            Process.waitpid(pid)
            return $?
          end
        rescue Timeout::Error
          if pid
            require 'rbconfig'
            if RbConfig::CONFIG['host_os'] !~ /mswin|mingw|cygwin/
              Process.kill('QUIT', pid)
              Process.kill('TERM', pid)
            else
              # win32 does not support SIGTERM
              Process.kill('INT', pid)
            end
            Process.kill('KILL', pid) rescue nil
          end
          raise BulkImportExecutionError, "Bulk Import execution timed out: #{@timeout} [sec]"
        end
      else
        system(*@cmd)
        return $?
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
      puts "Cannot find 'logging.properties' file in '#{Updater.jarfile_dest_path}' or " +
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

end
end
