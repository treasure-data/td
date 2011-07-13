
module TRD
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

  def import
    op = cmd_opt 'import', :db_name, :table_name, :files_

    op.banner << "\nsupported formats:\n"
    op.banner << "  apache\n"
    op.banner << "  syslog\n"
    op.banner << "  msgpack\n"
    op.banner << "  json\n"

    op.banner << "\noptions:\n"

    format = 'apache'
    time_key = nil

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

    db_name, table_name, *paths = op.cmd_parse

    conf = cmd_config
    api = cmd_api(conf)

    case format
    when 'json', 'msgpack'
      unless time_key
        $stderr.puts "-t, --time-key COL_NAME (e.g. '-t created_at') parameter is required for #{format} format"
        exit 1
      end
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

    find_table(api, db_name, table_name, :log)

    require 'zlib'

    files = paths.map {|path|
      if path == '-'
        $stdin
      elsif path =~ /\.gz$/
        Zlib::GzipReader.open(path)
      else
        File.open(path)
      end
    }

    require 'msgpack'
    require 'tempfile'
    #require 'thread'

    files.zip(paths).each {|file,path|
      import_log_file(file, path, api, db_name, table_name, parser)
    }

    puts "done."
  end

  private
  def import_log_file(file, path, api, db_name, table_name, parser)
    puts "importing #{path}..."

    out = Tempfile.new('trd-import')
    writer = Zlib::GzipWriter.new(out)

    n = 0
    x = 0
    parser.call(file, path) {|record|
      writer.write record.to_msgpack

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
          api.import(db_name, table_name, "msgpack.gz", out, size)

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
      api.import(db_name, table_name, "msgpack.gz", out, size)
    end

    puts "  imported #{n} entries from #{path}."

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
          m = @regexp.match(line)
          unless m
            raise "invalid log format at #{path}:#{i}"
          end

          record = {}

          cap = m.captures
          @names.each_with_index {|name,i|
            if value = cap[i]
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

          time = record[@time_key]
          unless time
            raise "record doesn't have '#{@time_key}' column"
          end

          t = Time.parse(time)
          record['time'] = t.to_i

          block.call(record)

        rescue
          $stderr.puts "  skipped: #{$!}: #{line.dump}"
        end
      }
    end
  end

  class MessagePackParser
    def initialize(time_key)
      require 'json'
      @time_key = time_key
    end

    def call(file, path, &block)
      i = 0
      MessagePack::Unpacker.new(file).each {|record|
        i += 1
        begin
          time = record[@time_key]
          unless time
            raise "record doesn't have '#{@time_key}' column"
          end

          t = Time.parse(time)
          record['time'] = t.to_i

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

