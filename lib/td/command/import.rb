
module TreasureData
module Command

  BASE_PATH = File.expand_path('../../..', File.dirname(__FILE__))

  JAVA_COMMAND = "java"
  JAVA_COMMAND_CHECK = "#{JAVA_COMMAND} -version >/dev/null 2>&1"
  JAVA_MAIN_CLASS = "com.treasure_data.bulk_import.BulkImportMain"
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

  def import_java_version(op)
    vfile = find_version_file[0]
    puts "td-bulk-import-java #{File.open(vfile, 'r').read}"
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
    bulk_importunfreeze(op)
  end

  private
  def import_by_java(subcmd)
    # check java runtime exists or not
    check_java

    # show help?
    show_help = ARGV.size == 0 || (ARGV.size == 1 || ARGV[0] =~ /^import:/)

    # configure java command-line arguments
    java_args = []
    java_args.concat build_sysprops
    java_args.concat ["-cp", find_td_bulk_import_jar]
    java_args << JAVA_MAIN_CLASS
    java_args << subcmd
    if show_help
      java_args << "--help"
    else
      java_args.concat ARGV
    end

    cmd = [JAVA_COMMAND] + JVM_OPTS + java_args
    system(*cmd)
  end

  private
  def check_java
    system(JAVA_COMMAND_CHECK)

    unless $?.success?
      $stderr.puts "Java is not installed. 'td import' command requires Java (version 1.6 or later)."
      $stderr.puts "Alternatively, you can use 'bulk_import' commands instead which is much slower."
      exit 1
    end
  end

  private
  def find_td_bulk_import_jar
    libjars = Dir.glob("#{BASE_PATH}/java/**/*.jar")
    found = libjars.find { |path| File.basename(path) =~ /^td-bulk-import/ }
    if found.nil?
      $stderr.puts "td-bulk-import.jar is not found."
      exit 1
    end
    found
  end

  private
  def build_sysprops
    sysprops = []

    # set apiserver
    set_sysprops_endpoint(sysprops)

    # set http_proxy
    set_sysprops_http_proxy(sysprops)

    # set configuration file for logging
    conf_file = try_find_logging_conf_file
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
  def try_find_logging_conf_file
    libjars = Dir.glob("#{BASE_PATH}/java/**/*.properties")
    libjars.find { |path| File.basename(path) =~ /^logging.properties/ }
  end

  private
  def find_version_file
    vfile = Dir.glob("#{BASE_PATH}/java/**/VERSION")
    vfile
  end

end
end
