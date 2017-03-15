require 'td/helpers'
require 'td/command/job'

module TreasureData
module Command
  HIVE_RESERVED_KEYWORDS = %W[
    TRUE FALSE ALL AND OR NOT LIKE ASC DESC ORDER BY GROUP WHERE FROM AS SELECT DISTINCT INSERT OVERWRITE
    OUTER JOIN LEFT RIGHT FULL ON PARTITION PARTITIONS TABLE TABLES TBLPROPERTIES SHOW MSCK DIRECTORY LOCAL
    TRANSFORM USING CLUSTER DISTRIBUTE SORT UNION LOAD DATA INPATH IS NULL CREATE EXTERNAL ALTER DESCRIBE
    DROP REANME TO COMMENT BOOLEAN TINYINT SMALLINT INT BIGINT FLOAT DOUBLE DATE DATETIME TIMESTAMP STRING
    BINARY ARRAY MAP REDUCE PARTITIONED CLUSTERED SORTED INTO BUCKETS ROW FORMAT DELIMITED FIELDS TERMINATED
    COLLECTION ITEMS KEYS LINES STORED SEQUENCEFILE TEXTFILE INPUTFORMAT OUTPUTFORMAT LOCATION TABLESAMPLE BUCKET OUT
    OF CAST ADD REPLACE COLUMNS RLIKE REGEXP TEMPORARY FUNCTION EXPLAIN EXTENDED SERDE WITH SERDEPROPERTIES LIMIT SET TBLPROPERTIES
  ]
  KEY_NUM_LIMIT = 512

  def table_create(op)
    type = nil

    op.on('-T', '--type TYPE', 'set table type (log)') {|s|
      unless s == 'log'
        raise "Unknown table type #{s.dump}. Supported types: log"
      end
      type = s.to_sym
    }
    db_name, table_name = op.cmd_parse

    API.validate_table_name(table_name)

    if HIVE_RESERVED_KEYWORDS.include?(table_name.upcase)
      $stderr.puts "* WARNING *"
      $stderr.puts "  '#{table_name}' is a reserved keyword in Hive. We recommend renaming the table."
      $stderr.puts "  For a list of all reserved keywords, see our FAQ: http://docs.treasure-data.com/articles/faq"
    end

    client = get_client

    begin
      client.create_log_table(db_name, table_name)
    rescue NotFoundError
      cmd_debug_error $!
      $stderr.puts "Database '#{db_name}' does not exist."
      $stderr.puts "Use '#{$prog} " + Config.cl_options_string + "db:create #{db_name}' to create the database."
      exit 1
    rescue AlreadyExistsError
      cmd_debug_error $!
      $stderr.puts "Table '#{db_name}.#{table_name}' already exists."
      exit 1
    end

    $stderr.puts "Table '#{db_name}.#{table_name}' is created."
  end

  def table_delete(op)
    force = false
    op.on('-f', '--force', 'never prompt', TrueClass) {|b|
      force = true
    }

    db_name, table_name = op.cmd_parse

    client = get_client

    begin
      unless force
        table = get_table(client, db_name, table_name)
        $stderr.print "Do you really delete '#{table_name}' in '#{db_name}'? [y/N]: "
        ok = nil
        while line = $stdin.gets
          line.strip!
          if line =~ /^y(?:es)?$/i
            ok = true
            break
          elsif line.empty? || line =~ /^n(?:o)?$/i
            break
          else
            $stderr.print "Type 'Y' or 'N': "
          end
        end
        unless ok
          $stderr.puts "canceled."
          exit 1
        end
      end
      client.delete_table(db_name, table_name)
    rescue NotFoundError
      cmd_debug_error $!
      $stderr.puts "Table '#{db_name}.#{table_name}' does not exist."
      $stderr.puts "Use '#{$prog} " + Config.cl_options_string + "table:list #{db_name}' to show list of the tables."
      exit 1
    end

    $stderr.puts "Table '#{db_name}.#{table_name}' is deleted."
  end

  def table_list(op)
    require 'parallel'

    format = 'table'
    num_threads = 4
    show_size_in_bytes = false

    op.on('-n', '--num_threads VAL', 'number of threads to get list in parallel') { |i|
      num_threads = Integer(i)
    }
    op.on('--show-bytes', 'show estimated table size in bytes') {
      show_size_in_bytes = true
    }
    set_render_format_option(op)

    db_name = op.cmd_parse

    client = get_client

    if db_name
      database = get_database(client, db_name)
      databases = [database]
    else
      databases = client.databases
    end

    # ref. https://github.com/treasure-data/td/issues/26
    should_number_format = [nil, "table"].include?(op.render_format)
    rows = []
    ::Parallel.each(databases, :in_threads => num_threads) {|db|
      begin
        if db.permission == :import_only
          next
        end
        db.tables.each {}
        db.tables.each {|table|
          pschema = table.schema.fields.map {|f|
            "#{f.name}:#{f.type}"
          }.join(', ')
          new_row = {
            :Database => db.name, :Table => table.name, :Type => table.type.to_s, :Count => (should_number_format ? TreasureData::Helpers.format_with_delimiter(table.count) : table.count),
            :Size => show_size_in_bytes ? TreasureData::Helpers.format_with_delimiter(table.estimated_storage_size) : table.estimated_storage_size_string,
            'Last import' => table.last_import ? table.last_import.localtime : nil,
            'Last log timestamp' => table.last_log_timestamp ? table.last_log_timestamp.localtime : nil,
            :Schema => pschema
          }

          rows << new_row
        }
      rescue APIError => e
        # ignores permission error because db:list shows all databases
        # even if the user can't access to tables in the database
        unless e.to_s =~ /not authorized/
          raise e
        end
      end
    }
    rows = rows.sort_by {|map|
      [map[:Database], map[:Type].size, map[:Table]]
    }

    fields = [:Database, :Table, :Type, :Count, :Size, 'Last import', 'Last log timestamp', :Schema]
    $stdout.puts cmd_render_table(rows, :fields => fields, :max_width => 500, :render_format => op.render_format)

    if rows.empty?
      if db_name
        if databases.first.permission == :import_only
          $stderr.puts "Database '#{db_name}' is import only, cannot list or create tables."
        else
          $stderr.puts "Database '#{db_name}' has no tables."
          $stderr.puts "Use '#{$prog} " + Config.cl_options_string + "table:create <db> <table>' to create a table."
        end
      elsif databases.empty?
        $stderr.puts "There are no databases."
        $stderr.puts "Use '#{$prog} " + Config.cl_options_string + "db:create <db>' to create a database."
      else
        $stderr.puts "There are no tables."
        $stderr.puts "Use '#{$prog} " + Config.cl_options_string + "table:create <db> <table>' to create a table."
      end
    end
  end

  def table_swap(op)
    db_name, table_name1, table_name2 = op.cmd_parse

    client = get_client

    table1 = get_table(client, db_name, table_name1)
    table2 = get_table(client, db_name, table_name2)

    client.swap_table(db_name, table_name1, table_name2)

    $stderr.puts "'#{db_name}.#{table_name1}' and '#{db_name}.#{table_name2}' are swapped."
  end

  def table_rename(op)
    overwrite = false
    op.on('--overwrite', 'replace existing dest table') { overwrite = true }
    database_name, from_table_name, dest_table_name = op.cmd_parse

    client = get_client
    database = get_database(client, database_name)

    unless table_exist?(database, from_table_name)
      raise ParameterConfigurationError, "From table `#{from_table_name}` isn't exist."
    end

    if table_exist?(database, dest_table_name)
      unless overwrite
        raise ParameterConfigurationError, "Dest table `#{dest_table_name}` is exist. If you want to overwrite dest table, you should set `overwrite` option."
      end
    else
      client.create_log_table(database_name, dest_table_name)
    end

    client.swap_table(database_name, from_table_name, dest_table_name)
    client.delete_table(database_name, from_table_name)

    $stderr.puts "'renamed from '#{database_name}.#{from_table_name}' to '#{database_name}.#{dest_table_name}'."
  rescue ParameterConfigurationError => e
    $stderr.puts e.message
    exit 1
  end

  def table_show(op)
    db_name, table_name = op.cmd_parse

    client = get_client

    table = get_table(client, db_name, table_name)

    $stdout.puts "Name        : #{table.db_name}.#{table.name}"
    $stdout.puts "Type        : #{table.type}"
    $stdout.puts "Count       : #{table.count}"
    # p table.methods.each {|m| $stdout.puts m}
    $stdout.puts "Schema      : ("
    table.schema.fields.each {|f|
      $stdout.puts "    #{f.name}:#{f.type}"
    }
    $stdout.puts ")"
  end

  def table_tail(op)
    count = 10
    pretty = nil

    op.on('-n', '--count N', 'number of logs to get', Integer) {|i|
      count = i
    }
    op.on('-P', '--pretty', 'pretty print', TrueClass) {|b|
      pretty = b
    }

    db_name, table_name = op.cmd_parse

    client = get_client

    table = get_table(client, db_name, table_name)

    rows = table.tail(count)

    require 'json'
    if pretty
      opts = {
        :indent => ' '*2,
        :object_nl => "\n",
        :space => ' '
      }
      rows.each {|row|
        $stdout.puts row.to_json(opts)
      }
    else
      rows.each {|row|
        $stdout.puts row.to_json
      }
    end
  end

  def table_partial_delete(op)
    from = nil
    to = nil
    wait = false
    pool_name = nil

    op.on('-t', '--to TIME', 'end time of logs to delete in Unix time >0 and multiple of 3600 (1 hour)') {|s|
      if s.to_i.to_s == s
        # UNIX time
        to = s.to_i
      else
        require 'time'
        to = Time.parse(s).to_i
      end
    }
    op.on('-f', '--from TIME', 'start time of logs to delete in Unix time >0 and multiple of 3600 (1 hour)') {|s|
      if s.to_i.to_s == s
        from = s.to_i
      else
        require 'time'
        from = Time.parse(s).to_i
      end
    }
    op.on('-w', '--wait', 'wait for the job to finish', TrueClass) {|b|
      wait = b
    }
    op.on('-O', '--pool-name NAME', 'specify resource pool by name') {|s|
      pool_name = s
    }

    db_name, table_name = op.cmd_parse

    unless from
      $stderr.puts "-f, --from TIME option is required"
      exit 1
    end

    unless to
      $stderr.puts "-t, --to TIME option is required"
      exit 1
    end

    if from % 3600 != 0 || to % 3600 != 0
      $stderr.puts "Time for the -f / --from and -t / --to options must either be a multiple of 3600 (1 hour)\n" +
                   "  or be expressed in Ruby time string format where the minutes and seconds are 0"
      exit 1
    end

    client = get_client

    table = get_table(client, db_name, table_name)

    opts = {}
    opts['pool_name'] = pool_name if pool_name
    job = client.partial_delete(db_name, table_name, to, from, opts)

    $stderr.puts "Partial delete job #{job.job_id} is queued."
    $stderr.puts "Use '#{$prog} " + Config.cl_options_string + "job:show #{job.job_id}' to show the status."

    if wait && !job.finished?
      wait_job(job)
      $stdout.puts "Status     : #{job.status}"
    end
  end

  def table_expire(op)
    db_name, table_name, expire_days = op.cmd_parse

    expire_days = expire_days.to_i
    if expire_days < 0
      $stderr.puts "Table expiration days must be greater or equal to 0."
      return
    end

    client = get_client
    client.update_expire(db_name, table_name, expire_days)

    if expire_days == 0
      $stdout.puts "Data expiration disabled for this table."
    else
      $stdout.puts "Table set to expire data older than #{expire_days} days."
    end
  end


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

    import_params = {time_key: time_key}

    op.on('--format FORMAT', "file format (default: #{format})") {|s|
      import_params[:format] = s
    }

    op.on('--apache', "same as --format apache; apache common log format") {
      import_params[:format] = 'apache'
    }

    op.on('--syslog', "same as --format syslog; syslog") {
      import_params[:format] = 'syslog'
    }

    op.on('--msgpack', "same as --format msgpack; msgpack stream format") {
      import_params[:format] = 'msgpack'
    }

    op.on('--json', "same as --format json; LF-separated json format") {
      import_params[:format] = 'json'
    }

    op.on('-t', '--time-key COL_NAME', "time key name for json and msgpack format (e.g. 'created_at')") {|s|
      import_params[:time_key] = s
    }

    op.on('--auto-create-table', "Create table and database if doesn't exist", TrueClass) { |b|
      auto_create = b
    }

    db_name, table_name, *paths = op.cmd_parse
    import_params[:db_name] = db_name
    import_params[:table_name] = table_name
    import_params[:paths] = paths

    client = get_client

    if auto_create
      create_database_and_table_if_not_exist(client, db_name, table_name)
    end

    do_table_import(client, import_params)
  end

  private
  def do_table_import(client, import_params)
    case import_params[:format]
    when 'json', 'msgpack'
      #unless time_key
      #  $stderr.puts "-t, --time-key COL_NAME (e.g. '-t created_at') parameter is required for #{format} format"
      #  exit 1
      #end
      if import_params[:format] == 'json'
        require 'json'
        require 'time'
        parser = JsonParser.new(import_params[:time_key])
      else
        parser = MessagePackParser.new(import_params[:time_key])
      end

    else  # apache, syslog
      regexp, names, time_format = IMPORT_TEMPLATES[import_params[:format]]
      if !regexp || !names || !time_format
        $stderr.puts "Unknown format '#{import_params[:format]}'"
        exit 1
      end
      parser = TextParser.new(names, regexp, time_format)
    end

    begin
      db = client.database(import_params[:db_name])
    rescue ForbiddenError => e
      $stdout.puts "Warning: database and table validation skipped - #{e.message}"
    else
      begin
        table = db.table(import_params[:table_name])
      rescue ForbiddenError => e
        $stdout.puts "Warning: table validation skipped - #{e.message}"
      end
    end

    require 'zlib'

    begin
      files = import_params[:paths].map {|path|
        if path == '-'
          $stdin
        elsif path =~ /\.gz$/
          require 'td/compat_gzip_reader'
          Zlib::GzipReader.open(path)
        else
          File.open(path)
        end
      }
    rescue Errno::ENOENT => e
      raise ImportError, e.message
    end

    require 'msgpack'
    require 'tempfile'
    #require 'thread'

    files.zip(import_params[:paths]).each {|file, path|
      import_log_file(file, path, client, import_params[:db_name], import_params[:table_name], parser)
    }

    $stdout.puts "done."
  end

  def import_log_file(file, path, client, db_name, table_name, parser)
    $stdout.puts "importing #{path}..."

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
      if n % 10000 == 0   # by records imported
        $stdout.puts "  imported #{n} entries from #{path}..."

      # TODO size
      elsif out.pos > 1024 * 1024   # by 1 MB chunks
        $stdout.puts "  imported #{n} entries from #{path}..."
        begin
          writer.finish
          size = out.pos
          out.pos = 0

          $stdout.puts "  uploading #{size} bytes..."
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

    # if there is anything parse but not imported yet
    if x != 0
      writer.finish
      size = out.pos
      out.pos = 0

      $stdout.puts "  uploading #{size} bytes..."
      # TODO upload on background thread
      client.import(db_name, table_name, "msgpack.gz", out, size)
    end

    # throw an exception if no record is imported
    if n == 0
      raise ImportError, "no valid record to import from #{path}"
    end

    $stdout.puts "  imported #{n} entries from #{path}."
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

  # Generic class for both JSON and MessagePack parsers to
  # reduce code duplication
  class StructuredParser
    def sanitize_record(record, &block)
      unless record.is_a?(Hash)
        raise "record must be a Hash"
      end

      time = record[@time_key]
      unless time
        raise "record doesn't have '#{@time_key}' column"
      end

      if record.size > KEY_NUM_LIMIT
        raise "record contains too many keys (#{record.size}, max allowed #{KEY_NUM_LIMIT})"
      end

      case time
      when Integer
        # do nothing
      else
        time = Time.parse(time.to_s).to_i
      end
      record['time'] = time

      block.call(record)
    end
    protected :sanitize_record
  end

  class JsonParser < StructuredParser
    def initialize(time_key)
      require 'json'
      @time_key = time_key
    end

    def call(file, path, &block)
      file.each_line {|line|
        begin
          record = JSON.parse(line)
          sanitize_record(record, &block)
        rescue
          $stderr.puts "  skipped: #{$!}: #{record.to_json}"
        end
      }
    end
  end

  class MessagePackParser < StructuredParser
    def initialize(time_key)
      require 'msgpack'
      @time_key = time_key
    end

    def call(file, path, &block)
      MessagePack::Unpacker.new(file).each {|record|
        begin
          sanitize_record(record, &block)
        rescue
          $stderr.puts "  skipped: #{$!}: #{record.to_json}"
        end
      }
    rescue EOFError
    end
  end

  require 'td/command/export'  # table:export
  require 'td/command/job'  # wait_job
end
end
