
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
                normalized_msgpack(record)
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
end
end

