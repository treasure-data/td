
module TreasureData
module Command

  BASE_PATH = File.expand_path('../../..', File.dirname(__FILE__))

  JAVA_COMMAND = "java"
  JAVA_MAIN_CLASS = "com.treasure_data.bulk_import.BulkImportMain"
  JAVA_HEAP_MAX_SIZE = "-Xmx1024m" # TODO

  APP_OPTION_PREPARE = "prepare"
  APP_OPTION_UPLOAD = "upload"

  def import_prepare(op)
    import_generic(APP_OPTION_PREPARE)
  end

  def import_upload(op)
    import_generic(APP_OPTION_UPLOAD)
  end

  private
  def import_generic(subcmd)
    puts "It requires Java version 1.6 or later. If Java is not installed yet, please use 'bulk_import' commands instead of this command."
    puts ""

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
    java_args << ARGV

    # TODO consider parameters including spaces; don't use join(' ')
    cmd = "#{JAVA_COMMAND} #{jvm_opts.join(' ')} #{java_opts.join(' ')} #{sysprops.join(' ')} #{java_args.join(' ')}"

    exec cmd
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
  def set_sysprops()
    sysprops = []

    # set http_proxy
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
  def find_logging_conf_file
    libjars = Dir.glob("#{BASE_PATH}/java/**/*.properties")
    found = libjars.find { |path| File.basename(path) =~ /^logging.properties/ }
    return nil if found.nil?
    logging_conf_file = libjars.delete(found)
    logging_conf_file
  end

end
end
