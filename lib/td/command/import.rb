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
    version = find_version_file
    puts "td-import-java #{File.open(version, 'r').read}"
  end

  def import_jar_update(op)
    check_n_update_jar(false)
  end

  def import_prepare(op)
    import_by_java('prepare')
  end

  def import_upload(op)
    import_by_java('upload')
  end

  def import_auto(op)
    import_by_java('auto')
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
  def import_by_java(subcmd)
    check_n_update_jar(true)

    # check java runtime exists or not
    check_java

    # show help?
    show_help = ARGV.size == 0 || (ARGV.size == 1 || ARGV[0] =~ /^import:/)

    # configure java command-line arguments
    java_args = []
    java_args.concat build_sysprops
    java_args.concat ["-cp", find_td_import_jar]
    java_args << JAVA_MAIN_CLASS
    java_args << subcmd
    if show_help
      java_args << "--help"
    else
      java_args.concat ARGV
    end

    cmd = [JAVA_COMMAND] + JVM_OPTS + java_args
    system(*cmd)
    if $?.exitstatus != 0
      raise BulkImportExecutionError,
            "Bulk Import returned error #{$?.exitstatus}. Please check the 'td-bulk-import.log' logfile for details."
    end
  end

  private
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
    endpoint = Config.endpoint
    if endpoint
      require 'uri'

      uri = URI.parse(endpoint)

      case uri.scheme
      when 'http', 'https'
        host = uri.host
        port = uri.port
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
        port = 443 if port == 0

        # TODO support ssl
        ssl = (port == 443)
      end

      sysprops << "-Dtd.api.server.scheme=#{ssl ? 'https' : 'http'}://"
      sysprops << "-Dtd.api.server.host=#{host}"
      sysprops << "-Dtd.api.server.port=#{port}"
    end
  end

  private
  def set_sysprops_http_proxy(sysprops)
    http_proxy = ENV['HTTP_PROXY']
    if http_proxy
      if http_proxy =~ /\Ahttp:\/\/(.*)\z/
        http_proxy = $~[1]
      end
      proxy_host, proxy_port = http_proxy.split(':', 2)
      proxy_port = (proxy_port ? proxy_port.to_i : 80)

      sysprops << "-Dhttp.proxyHost=#{proxy_host}"
      sysprops << "-Dhttp.proxyPort=#{proxy_port}"
    end
  end

end
end
