
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

    op.banner << "\noptions:\n"

    format = 'apache'

    op.on('--format FORMAT', "file format (default: #{format})") {|s|
      format = s
    }

    op.on('--apache', "same as --format apache; apache common log format") {
      format = 'apache'
    }

    db_name, table_name, *paths = op.cmd_parse

    conf = cmd_config
    api = cmd_api(conf)

    regexp, names, time_format = IMPORT_TEMPLATES[format]
    if !regexp || !names || !time_format
      $stderr.puts "Unknown format '#{format}'"
      exit 1
    end

    find_table(api, db_name, table_name, :log)

    files = paths.map {|path|
      if path == '-'
        $stdin
      else
        File.open(path)
      end
    }

    require 'zlib'
    require 'msgpack'
    require 'tempfile'
    #require 'thread'

    files.zip(paths).each {|file,path|
      puts "importing #{path}..."

      out = Tempfile.new('trd-import')
      def out.close
        # don't remove the file on close
        super(false)
      end
      writer = Zlib::GzipWriter.new(out)

      begin
        import_log_file(regexp, names, time_format, file, path, writer)

        writer.finish
        size = out.pos
        out.pos = 0

        # TODO upload on background thread
        puts "uploading #{path}..."
        api.import(db_name, table_name, "msgpack.gz", out, out.lstat.size)

      ensure
        writer.close unless writer.closed?
        out.close unless out.closed?
        File.unlink(out.path) rescue nil
      end
    }

    puts "done."
  end

  private
  def import_log_file(regexp, names, time_format, file, path, writer)
    i = 0
    n = 0
    file.each_line {|line|
      i += 1
      begin
        m = regexp.match(line)
        unless m
          raise "invalid log format at #{path}:#{i}"
        end

        record = {}

        cap = m.captures
        names.each_with_index {|name,i|
          if value = cap[i]
            if name == "time"
              time = parse_time(value, time_format).to_i
            end
            record[name] = value
          end
        }

        writer.write record.to_msgpack

        n += 1
        if n % 10000 == 0
          puts "  imported #{n} entries from #{path}..."
        end
      rescue
        $stderr.puts "#{$!}: #{line.dump}"
      end
    }
    puts "  imported #{n} entries from #{path}."
  end

  require 'date'  # DateTime#strptime
  require 'time'  # Time#strptime, Time#parse

  if Time.respond_to?(:strptime)
    def parse_time(value, time_format)
      Time.strptime(value, time_format)
    end
  else
    def parse_time(value, time_format)
      Time.parse(DateTime.strptime(value, time_format).to_s)
    end
  end
end
end

