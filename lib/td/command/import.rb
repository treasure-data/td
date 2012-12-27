
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

    require 'td/file_reader'
    reader = FileReader.new
    reader.set_format_template(format)
    reader.opts[:time_column] = time_key
    reader.opts[:all_string] = true # old behavior doesn't convert
    reader.opts[:compress] = 'plain' # See following paths.map routine

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
      import_log_file(file, path, client, db_name, table_name, reader)
    }

    puts "done."
  end

  private
  def import_log_file(file, path, client, db_name, table_name, reader)
    error = Proc.new { |reason, data|
      begin
        $stderr.puts "  skipped: #{reason}: #{data.dump}"
      rescue
        $stderr.puts "  skipped: #{reason}"
      end
    }

    puts "importing #{path}..."

    out = Tempfile.new('td-import')
    out.binmode if out.respond_to?(:binmode)

    writer = Zlib::GzipWriter.new(out)

    n = 0
    x = 0
    reader.parse(file, error) { |record|
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

  ensure
    out.close rescue nil
    writer.close rescue nil
  end
end
end

