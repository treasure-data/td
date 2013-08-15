
module TreasureData
module Command

  IMPORT_TEMPLATES = {
    'apache' => [
                  /^([^ ]*) [^ ]* ([^ ]*) \[([^\]]*)\] "(\S+)(?: +([^ ]*) +\S*)?" ([^ ]*) ([^ ]*)(?: "([^\"]*)" "([^\"]*)")?$/,
                  ['host', 'user', 'time', 'method', 'path', 'code', 'size', 'referer', 'agent'],
                  "%d/%b/%Y:%H:%M:%S %z"],
    'syslog' => [
                  /^([^ ]* [^ ]* [^ ]*) ([^ ]*) ([a-zA-Z0-9_\/\.\-]*)(?:\[([0-9]+)\])?[^\:]*\: *(.*)$/,
                  ['time', 'host', 'ident', 'pid', 'message'],
                  "%b %d %H:%M:%S"],
  }

  # TODO import-item
  # TODO tail

  def table_import(op)
    op.banner << "\nsupported formats:\n"
    op.banner << "  apache\n"
    op.banner << "  syslog\n"
    op.banner << "  msgpack\n"
    op.banner << "  json\n"

    format = 'apache'
    time_key = 'time'
    auto_create = false

    op.on('--format FORMAT', "file format (default: #{format})") {|s|
      format = s
    }

    op.on('--apache', "same as --format apache; apache common log format") {
      format = 'apache'
    }

    op.on('--syslog', "same as --format syslog; syslog") {
      format = 'syslog'
    }

    op.on('--msgpack', "same as --format msgpack; msgpack stream format") {
      format = 'msgpack'
    }

    op.on('--json', "same as --format json; LF-separated json format") {
      format = 'json'
    }

    op.on('-t', '--time-key COL_NAME', "time key name for json and msgpack format (e.g. 'created_at')") {|s|
      time_key = s
    }

    op.on('--auto-create-table', "Create table and database if doesn't exist", TrueClass) { |b|
      auto_create = b
    }

    db_name, table_name, *paths = op.cmd_parse

    client = get_client

    if auto_create
      # Merge with db_create and table_create after refactoring
      API.validate_database_name(db_name)
      begin
        client.create_database(db_name)
        $stderr.puts "Database '#{db_name}' is created."
      rescue AlreadyExistsError
      end

      API.validate_table_name(table_name)
      begin
        client.create_log_table(db_name, table_name)
        $stderr.puts "Table '#{db_name}.#{table_name}' is created."
      rescue AlreadyExistsError
      end
    end

    case format
    when 'json', 'msgpack'
      #unless time_key
      #  $stderr.puts "-t, --time-key COL_NAME (e.g. '-t created_at') parameter is required for #{format} format"
      #  exit 1
      #end
      if format == 'json'
        require 'json'
        require 'time'
        parser = JsonParser.new(time_key)
      else
        parser = MessagePackParser.new(time_key)
      end

    else
      regexp, names, time_format = IMPORT_TEMPLATES[format]
      if !regexp || !names || !time_format
        $stderr.puts "Unknown format '#{format}'"
        exit 1
      end
      parser = TextParser.new(names, regexp, time_format)
    end

    get_table(client, db_name, table_name)

    require 'zlib'

    files = paths.map {|path|
      if path == '-'
        $stdin
      elsif path =~ /\.gz$/
        require 'td/compat_gzip_reader'
        Zlib::GzipReader.open(path)
      else
        File.open(path)
      end
    }

    require 'msgpack'
    require 'tempfile'
    #require 'thread'

    files.zip(paths).each {|file,path|
      import_log_file(file, path, client, db_name, table_name, parser)
    }

    puts "done."
  end

  private
  def import_log_file(file, path, client, db_name, table_name, parser)
    puts "importing #{path}..."

    out = Tempfile.new('td-import')
    out.binmode if out.respond_to?(:binmode)

    writer = Zlib::GzipWriter.new(out)

    n = 0
    x = 0
    has_bignum = false
    parser.call(file, path) {|record|
      entry = begin
                record.to_msgpack
              rescue RangeError
                has_bignum = true
                TreasureData::API.normalized_msgpack(record)
              end
      writer.write entry

      n += 1
      x += 1
      if n % 10000 == 0
        puts "  imported #{n} entries from #{path}..."

      elsif out.pos > 1024*1024  # TODO size
        puts "  imported #{n} entries from #{path}..."
        begin
          writer.finish
          size = out.pos
          out.pos = 0

          puts "  uploading #{size} bytes..."
          client.import(db_name, table_name, "msgpack.gz", out, size)

          out.truncate(0)
          out.pos = 0
          x = 0
          writer = Zlib::GzipWriter.new(out)
        rescue
          $stderr.puts "  #{$!}"
          return 1 # TODO error
        end
      end
    }

    if x != 0
      writer.finish
      size = out.pos
      out.pos = 0

      puts "  uploading #{size} bytes..."
      # TODO upload on background thread
      client.import(db_name, table_name, "msgpack.gz", out, size)
    end

    puts "  imported #{n} entries from #{path}."
    $stderr.puts normalized_message if has_bignum
  ensure
    out.close rescue nil
    writer.close rescue nil
  end

  require 'date'  # DateTime#strptime
  require 'time'  # Time#strptime, Time#parse

  class TextParser
    def initialize(names, regexp, time_format)
      @names = names
      @regexp = regexp
      @time_format = time_format
    end

    def call(file, path, &block)
      i = 0
      file.each_line {|line|
        i += 1
        begin
          line.rstrip!
          m = @regexp.match(line)
          unless m
            raise "invalid log format at #{path}:#{i}"
          end

          record = {}

          cap = m.captures
          @names.each_with_index {|name,cap_i|
            if value = cap[cap_i]
              if name == "time"
                value = parse_time(value).to_i
              end
              record[name] = value
            end
          }

          block.call(record)

        rescue
          $stderr.puts "  skipped: #{$!}: #{line.dump}"
        end
      }
    end

    if Time.respond_to?(:strptime)
      def parse_time(value)
        Time.strptime(value, @time_format)
      end
    else
      def parse_time(value)
        Time.parse(DateTime.strptime(value, @time_format).to_s)
      end
    end
  end

  class JsonParser
    def initialize(time_key)
      require 'json'
      @time_key = time_key
    end

    def call(file, path, &block)
      i = 0
      file.each_line {|line|
        i += 1
        begin
          record = JSON.parse(line)

          unless record.is_a?(Hash)
            raise "record must be a Hash"
          end

          time = record[@time_key]
          unless time
            raise "record doesn't have '#{@time_key}' column"
          end

          case time
          when Integer
            # do nothing
          else
            time = Time.parse(time.to_s).to_i
          end
          record['time'] = time

          block.call(record)

        rescue
          $stderr.puts "  skipped: #{$!}: #{line.dump}"
        end
      }
    end
  end

  class MessagePackParser
    def initialize(time_key)
      require 'msgpack'
      @time_key = time_key
    end

    def call(file, path, &block)
      i = 0
      MessagePack::Unpacker.new(file).each {|record|
        i += 1
        begin
          unless record.is_a?(Hash)
            raise "record must be a Hash"
          end

          time = record[@time_key]
          unless time
            raise "record doesn't have '#{@time_key}' column"
          end

          case time
          when Integer
            # do nothing
          else
            time = Time.parse(time.to_s).to_i
          end
          record['time'] = time

          block.call(record)

        rescue
          $stderr.puts "  skipped: #{$!}: #{record.to_json}"
        end
      }
    rescue EOFError
    end
  end

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
