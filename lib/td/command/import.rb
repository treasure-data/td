
module TreasureData
module Command

  BASE_PATH = File.expand_path('../../..', File.dirname(__FILE__))

  JAVA_COMMAND = "java"
  JAVA_COMMAND_CHECK = "#{JAVA_COMMAND} -version"
  JAVA_MAIN_CLASS = "com.treasure_data.bulk_import.BulkImportMain"
  JAVA_HEAP_MAX_SIZE = "-Xmx1024m" # TODO

  APP_OPTION_PREPARE = "prepare"
  APP_OPTION_UPLOAD = "upload"

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

  def import_prepare(op)
    import_generic(APP_OPTION_PREPARE)
  end

  def import_upload(op)
    import_generic(APP_OPTION_UPLOAD)
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
    bulk_importunfreeze(op)
  end

  private
  def import_generic(subcmd)
    # has java runtime
    check_java

    # show help
    show_help = ARGV.size == 0 || (ARGV.size == 1 || ARGV[0] =~ /^import:/)

    # configure jvm options
    jvm_opts = [ JAVA_HEAP_MAX_SIZE ]

    # configure java options
    java_opts = [ "-cp \"#{find_td_bulk_import_jar()}\"" ]

    # configure system properties
    sysprops = set_sysprops()

    # configure java command-line arguments
    java_args = []
    java_args << JAVA_MAIN_CLASS
    java_args << subcmd
    if show_help
      java_args << "--help"
    else
      java_args << ARGV
    end

    # TODO consider parameters including spaces; don't use join(' ')
    cmd = "#{JAVA_COMMAND} #{jvm_opts.join(' ')} #{java_opts.join(' ')} #{sysprops.join(' ')} #{java_args.join(' ')}"
    exec cmd
  end

  private
  def check_java
    system(JAVA_COMMAND_CHECK)

    unless $?.success?
      $stderr.puts "Java is not installed. 'td import' command requires Java (version 1.6 or later). If Java is not installed yet, please use 'bulk_import' commands instead of this command."
      exit 1
    end
  end

  private
  def find_td_bulk_import_jar
    libjars = Dir.glob("#{BASE_PATH}/java/**/*.jar")
    found = libjars.find { |path| File.basename(path) =~ /^td-bulk-import/ }
    if found.nil?
      $stderr.puts "td-bulk-import.jar is not found."
      exit
    end
    td_bulk_import_jar = libjars.delete(found)
    td_bulk_import_jar
  end

  private
  def set_sysprops
    sysprops = []

    # set apiserver
    set_sysprops_endpoint(sysprops)

    # set http_proxy
    set_sysprops_http_proxy(sysprops)

    # set configuration file for logging
    conf_file = find_logging_conf_file
    if conf_file
      sysprops << "-Djava.util.logging.config.file=#{conf_file}"
    end

    # set API key
    sysprops << "-Dtd.api.key=#{TreasureData::Config.apikey}"

    sysprops
  end

  private
  def set_sysprops_endpoint(sysprops)
    endpoint = ENV['TD_API_SERVER']
    if endpoint
      require 'uri'

      uri = URI.parse(endpoint)

      case uri.scheme
      when 'http', 'https'
        host = uri.host
        port = uri.port
        ssl = uri.scheme == 'https'

        port = 80 if port == 443 and ssl
      else
        if uri.port
          # invalid URI
          raise "Invalid endpoint: #{endpoint}"
        end

        # generic URI
        host, port = endpoint.split(':', 2)
        port = port.to_i
        # TODO support ssl
        port = 80 if port == 0
        ssl = false
      end

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

  private
  def find_logging_conf_file
    libjars = Dir.glob("#{BASE_PATH}/java/**/*.properties")
    found = libjars.find { |path| File.basename(path) =~ /^logging.properties/ }
    return nil if found.nil?
    logging_conf_file = libjars.delete(found)
    logging_conf_file
  end

end
end
