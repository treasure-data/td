
module TreasureData
module Command

  def bulk_import_list(op)
    op.cmd_parse

    client = get_client

    bis = client.bulk_imports

    rows = []
    has_org = false
    bis.each {|bi|
      rows << {:Name=>bi.name, :Table=>"#{bi.database}.#{bi.table}", :Status=>bi.status.to_s.capitalize, :Frozen=>bi.upload_frozen? ? 'Frozen' : '', :JobID=>bi.job_id, :"Valid Parts"=>bi.valid_parts, :"Error Parts"=>bi.error_parts, :"Valid Records"=>bi.valid_records, :"Error Records"=>bi.error_records, :Organization=>bi.org_name}
      has_org = true if bi.org_name
    }

    puts cmd_render_table(rows, :fields => gen_table_fields(has_org, [:Name, :Table, :Status, :Frozen, :JobID, :"Valid Parts", :"Error Parts", :"Valid Records", :"Error Records"]), :max_width=>200)

    if rows.empty?
      $stderr.puts "There are no bulk import sessions."
      $stderr.puts "Use '#{$prog} bulk_import:create <name> <db> <table>' to create a session."
    end
  end

  def bulk_import_create(op)
    org = nil

    op.on('-g', '--org ORGANIZATION', "create the bulk import session under this organization") {|s|
      org = s
    }

    name, db_name, table_name = op.cmd_parse

    client = get_client

    table = get_table(client, db_name, table_name)

    opts = {}
    opts['organization'] = org if org
    client.create_bulk_import(name, db_name, table_name, opts)

    $stderr.puts "Bulk import session '#{name}' is created."
  end

  def bulk_import_delete(op)
    name = op.cmd_parse

    client = get_client

    begin
      client.delete_bulk_import(name)
    rescue NotFoundError
      cmd_debug_error $!
      $stderr.puts "Bulk import session '#{name}' does not exist."
      exit 1
    end

    $stderr.puts "Bulk import session '#{name}' is deleted."
  end

  def bulk_import_show(op)
    name = op.cmd_parse

    client = get_client

    bis = client.bulk_imports
    bi = bis.find {|bi| name == bi.name }
    unless bi
      $stderr.puts "Bulk import session '#{name}' does not exist."
      $stderr.puts "Use '#{$prog} bulk_import:create <name> <db> <table>' to create a session."
      exit 1
    end

    $stderr.puts "Organization : #{bi.org_name}"
    $stderr.puts "Name         : #{bi.name}"
    $stderr.puts "Database     : #{bi.database}"
    $stderr.puts "Table        : #{bi.table}"
    $stderr.puts "Status       : #{bi.status.to_s.capitalize}"
    $stderr.puts "Frozen       : #{bi.upload_frozen?}"
    $stderr.puts "JobID        : #{bi.job_id}"
    $stderr.puts "Valid Records: #{bi.valid_records}"
    $stderr.puts "Error Records: #{bi.error_records}"
    $stderr.puts "Valid Parts  : #{bi.valid_parts}"
    $stderr.puts "Error Parts  : #{bi.error_parts}"
    $stderr.puts "Uploaded Parts :"

    list = client.list_bulk_import_parts(name)
    list.each {|name|
      puts "  #{name}"
    }
  end

  # obsoleted
  def bulk_import_upload_part(op)
    retry_limit = 10
    retry_wait = 1

    name, part_name, path = op.cmd_parse

    File.open(path, "rb") {|io|
      bulk_import_upload_impl(name, part_name, io, io.size, retry_limit, retry_wait)
    }

    $stderr.puts "Part '#{part_name}' is uploaded."
  end

  def bulk_import_upload_parts(op)
    retry_limit = 10
    retry_wait = 1
    suffix_count = 0
    part_prefix = ""
    auto_perform = false
    parallel = 2

    op.on('-P', '--prefix NAME', 'add prefix to parts name') {|s|
      part_prefix = s
    }
    op.on('-s', '--use-suffix COUNT', 'use COUNT number of . (dots) in the source file name to the parts name', Integer) {|i|
      suffix_count = i
    }
    op.on('--auto-perform', 'perform bulk import job automatically', TrueClass) {|b|
      auto_perform = b
    }
    op.on('--parallel NUM', 'perform uploading in parallel (default: 2; max 8)', Integer) {|i|
      parallel = i
    }

    name, *files = op.cmd_parse

    parallel = 1 if parallel <= 1
    parallel = 8 if parallel >= 8

    threads = (1..parallel).map {|i|
      Thread.new do
        errors = []
        until files.empty?
          ifname = files.shift
          basename = File.basename(ifname)
          begin
            part_name = part_prefix + basename.split('.')[0..suffix_count].join('.')

            File.open(ifname, "rb") {|io|
              size = io.size
              $stderr.write "Uploading '#{ifname}' -> '#{part_name}'... (#{size} bytes)\n"

              bulk_import_upload_impl(name, part_name, io, size, retry_limit, retry_wait)
            }
          rescue
            errors << [ifname, $!]
          end
        end
        errors
      end
    }

    errors = []
    threads.each {|t|
      errors.concat t.value
    }

    unless errors.empty?
      $stderr.puts "failed to upload #{errors.size} files."
      $stderr.puts "backtraces:"
      errors.each {|(ifname,ex)|
        $stderr.puts "  #{ifname}: #{ex}"
        ex.backtrace.each {|bt|
          $stderr.puts "      #{ifname}: #{bt}"
        }
      }
      $stderr.puts "files:"
      ifnames = errors.map {|(ifname,ex)| ifname }
      ifnames.each {|ifname|
        $stderr.puts "  #{ifname}"
      }
      $stderr.puts "You can retry uploading by following command:"
      $stderr.puts "td bulk_import:upload_parts #{name} #{ifnames.map {|ifname| "'#{ifname}'" }.join(' ')}"
      exit 1
    end

    $stderr.puts "done."

    if auto_perform
      client = get_client
      job = client.perform_bulk_import(name)

      $stderr.puts "Job #{job.job_id} is queued."
      $stderr.puts "Use '#{$prog} job:show [-w] #{job.job_id}' to show the status."
    end
  end

  # obsoleted
  def bulk_import_delete_part(op)
    name, part_name = op.cmd_parse

    client = get_client

    client.bulk_import_delete_part(name, part_name)

    $stderr.puts "Part '#{part_name}' is deleted."
  end

  def bulk_import_delete_parts(op)
    part_prefix = ""

    op.on('-P', '--prefix NAME', 'add prefix to parts name') {|s|
      part_prefix = s
    }

    name, *part_names = op.cmd_parse

    client = get_client

    part_names.each {|part_name|
      part_name = part_prefix + part_name

      $stderr.puts "Deleting '#{part_name}'..."
      client.bulk_import_delete_part(name, part_name)
    }

    $stderr.puts "done."
  end

  def bulk_import_perform(op)
    wait = false
    force = false

    op.on('-w', '--wait', 'wait for finishing the job', TrueClass) {|b|
      wait = b
    }
    op.on('-f', '--force', 'force start performing', TrueClass) {|b|
      force = b
    }

    name = op.cmd_parse

    client = get_client

    unless force
      bis = client.bulk_imports
      bi = bis.find {|bi| name == bi.name }
      if bi
        if bi.status == 'performing'
          $stderr.puts "Bulk import session '#{name}' is already performing."
          $stderr.puts "Add '-f' option to force start."
          $stderr.puts "Use '#{$prog} job:kill #{bi.job_id}' to cancel the last trial."
          exit 1
        elsif bi.status == 'ready'
          $stderr.puts "Bulk import session '#{name}' is already ready to commit."
          $stderr.puts "Add '-f' option to force start."
          exit 1
        end
      end
    end

    job = client.perform_bulk_import(name)

    $stderr.puts "Job #{job.job_id} is queued."
    $stderr.puts "Use '#{$prog} job:show [-w] #{job.job_id}' to show the status."

    if wait
      require 'td/command/job'  # wait_job
      wait_job(job)
    end
  end

  def bulk_import_commit(op)
    name = op.cmd_parse

    client = get_client

    job = client.commit_bulk_import(name)

    $stderr.puts "Bulk import session '#{name}' started to commit."
  end

  def bulk_import_error_records(op)
    name = op.cmd_parse

    client = get_client

    bis = client.bulk_imports
    bi = bis.find {|bi| name == bi.name }
    unless bi
      $stderr.puts "Bulk import session '#{name}' does not exist."
      $stderr.puts "Use '#{$prog} bulk_import:create <name> <db> <table>' to create a session."
      exit 1
    end

    if bi.status == "uploading" || bi.status == "performing"
      $stderr.puts "Bulk import session '#{name}' is not performed."
      $stderr.puts "Use '#{$prog} bulk_import:perform <name>' to run."
      exit 1
    end

    require 'yajl'
    client.bulk_import_error_records(name) {|r|
      puts Yajl.dump(r)
    }
  end

  def bulk_import_freeze(op)
    name = op.cmd_parse

    client = get_client

    client.freeze_bulk_import(name)

    $stderr.puts "Bulk import session '#{name}' is frozen."
  end

  def bulk_import_unfreeze(op)
    name = op.cmd_parse

    client = get_client

    client.unfreeze_bulk_import(name)

    $stderr.puts "Bulk import session '#{name}' is unfrozen."
  end

  PART_SPLIT_SIZE = 16*1024*1024

  def bulk_import_prepare_parts(op)
    outdir = nil
    split_size_kb = PART_SPLIT_SIZE / 1024  # kb

    require 'td/file_reader'
    reader = FileReader.new
    reader.init_optparse(op)

    op.on('-s', '--split-size SIZE_IN_KB', "size of each parts (default: #{split_size_kb})", Integer) {|i|
      split_size_kb = i
    }
    op.on('-o', '--output DIR', 'output directory') {|s|
      outdir = s
    }

    files = op.cmd_parse

    # TODO ruby 1.9
    files = [files] unless files.is_a?(Array)

    unless outdir
      $stderr.puts "-o, --output DIR option is required."
      exit 1
    end

    split_size = split_size_kb * 1024

    require 'fileutils'
    FileUtils.mkdir_p(outdir)

    require 'yajl'
    require 'msgpack'
    require 'zlib'

    error = Proc.new {|reason,data|
      begin
        $stderr.puts "#{reason}: #{Yajl.dump(data)}"
      rescue
        $stderr.puts "#{reason}"
      end
    }
    has_bignum = false

    # TODO multi process
    files.each {|ifname|
      $stderr.puts "Processing #{ifname}..."
      record_num = 0

      basename = File.basename(ifname).sub(/\.(?:csv|tsv|json|msgpack)(?:\.gz)?$/i,'').split('.').join('_')
      File.open(ifname) {|io|
        of_index = 0
        out = nil
        zout = nil
        begin
          reader.parse(io, error) {|record|
            if zout == nil
              ofname = "#{basename}_#{of_index}.msgpack.gz"
              $stderr.puts "  Preparing part \"#{basename}_#{of_index}\"..."
              out = File.open("#{outdir}/#{ofname}", 'wb')
              zout = Zlib::GzipWriter.new(out)

              t = record['time']
              $stderr.puts "  sample: #{Time.at(t).utc} #{Yajl.dump(record)}"
            end

            entry = begin
                      record.to_msgpack
                    rescue RangeError
                      has_bignum = true
                      normalized_msgpack(record)
                    end
            zout.write(entry)
            record_num += 1

            if out.size > split_size
              zout.close
              of_index += 1
              out = nil
              zout = nil
            end
          }
        ensure
          if zout
            zout.close
            zout = nil
          end
        end
        $stderr.puts "  #{ifname}: #{record_num} entries."
      }
    }

    $stderr.puts normalized_message if has_bignum
  end

  def bulk_import_upload_parts2(op)
    opts = upload_parts2_config(op)

    # java command
    javacmd = 'java'

    # make jvm options
    jvm_opts = []
    jvm_opts << "-Xmx1024m" # TODO

    # find java/*.jar and td.jar
    base_path = File.expand_path('../../..', File.dirname(__FILE__)) # TODO
    libjars = Dir.glob("#{base_path}/java/**/*.jar")
    found = libjars.find { |path| File.basename(path) =~ /^td-bulk-import/ }
    td_command_jar = libjars.delete(found)

    # make application options
    app_opts = []
    app_opts << "-cp \"#{td_command_jar}\""

    # make system properties
    sysprops = []
    sysprops.concat(upload_parts2_sysprops(opts))

    # make application arguments
    app_args = []
    app_args << 'com.treasure_data.bulk_import.Main'
    app_args << 'upload_parts'
    app_args << opts[21]

    # TODO consider parameters including spaces; don't use join(' ')
    command = "#{javacmd} #{jvm_opts.join(' ')} #{app_opts.join(' ')} #{sysprops.join(' ')} #{app_args.join(' ')}"

    exec command
  end

  def bulk_import_prepare_parts2(op)
    opts = prepare_parts2_config(op)

    # java command
    javacmd = 'java'

    # make jvm options
    jvm_opts = []
    jvm_opts << "-Xmx1024m" # TODO

    # find java/*.jar and td.jar
    base_path = File.expand_path('../../..', File.dirname(__FILE__)) # TODO
    libjars = Dir.glob("#{base_path}/java/**/*.jar")
    found = libjars.find { |path| File.basename(path) =~ /^td-bulk-import/ }
    td_command_jar = libjars.delete(found)

    # make application options
    app_opts = []
    app_opts << "-cp \"#{td_command_jar}\""

    # make system properties
    sysprops = []
    sysprops.concat(prepare_parts2_sysprops(opts))

    # make application arguments
    app_args = []
    app_args << 'com.treasure_data.bulk_import.Main'
    app_args << 'prepare_parts'
    app_args << opts[18]

    # TODO consider parameters including spaces; don't use join(' ')
    command = "#{javacmd} #{jvm_opts.join(' ')} #{app_opts.join(' ')} #{sysprops.join(' ')} #{app_args.join(' ')}"

    exec command
  end

  private
  def prepare_parts2_sysprops(opts)
    sysprops = []
    sysprops << "-Dtd.bulk_import.prepare_parts.format=#{opts[0]}"
    sysprops << "-Dtd.bulk_import.prepare_parts.compression=#{opts[1]}"
    sysprops << "-Dtd.bulk_import.prepare_parts.encoding=#{opts[2]}"
    sysprops << "-Dtd.bulk_import.prepare_parts.time-column=#{opts[3]}"
    sysprops << "-Dtd.bulk_import.prepare_parts.time-format=#{opts[4]}"
    sysprops << "-Dtd.bulk_import.prepare_parts.time-value=#{opts[5].to_s}" if opts[5]
    sysprops << "-Dtd.bulk_import.prepare_parts.output-dir=#{opts[6]}"
    sysprops << "-Dtd.bulk_import.prepare_parts.split-size=#{opts[7]}"
    sysprops << "-Dtd.bulk_import.prepare_parts.error-record-output=#{opts[8]}" if opts[8]
    sysprops << "-Dtd.bulk_import.prepare_parts.dry-run=#{opts[9]}"
    sysprops << "-Dtd.bulk_import.prepare_parts.delimiter=#{opts[10]}"
    sysprops << "-Dtd.bulk_import.prepare_parts.newline=#{opts[11]}"
    sysprops << "-Dtd.bulk_import.prepare_parts.column-header=#{opts[12]}" if opts[3]
    sysprops << "-Dtd.bulk_import.prepare_parts.columns=#{opts[13]}" if opts[1]
    sysprops << "-Dtd.bulk_import.prepare_parts.column-types=#{opts[14]}" if opts[2]
    sysprops << "-Dtd.bulk_import.prepare_parts.type-conversion-error=#{opts[15]}" if opts[15]
    sysprops << "-Dtd.bulk_import.prepare_parts.exclude-columns=#{opts[16]}"
    sysprops << "-Dtd.bulk_import.prepare_parts.only-columns=#{opts[17]}"
    sysprops
  end

  private
  def upload_parts2_sysprops(opts)
    sysprops = []
    sysprops << "-Dtd.bulk_import.prepare_parts.format=#{opts[0]}"
    sysprops << "-Dtd.bulk_import.prepare_parts.compression=#{opts[1]}"
    sysprops << "-Dtd.bulk_import.prepare_parts.encoding=#{opts[2]}"
    sysprops << "-Dtd.bulk_import.prepare_parts.time-column=#{opts[3]}"
    sysprops << "-Dtd.bulk_import.prepare_parts.time-format=#{opts[4]}"
    sysprops << "-Dtd.bulk_import.prepare_parts.time-value=#{opts[5].to_s}" if opts[5]
    sysprops << "-Dtd.bulk_import.prepare_parts.output-dir=#{opts[6]}"
    sysprops << "-Dtd.bulk_import.prepare_parts.split-size=#{opts[7]}"
    sysprops << "-Dtd.bulk_import.prepare_parts.error-record-output=#{opts[8]}" if opts[8]
    sysprops << "-Dtd.bulk_import.prepare_parts.dry-run=#{opts[9]}"
    sysprops << "-Dtd.bulk_import.prepare_parts.delimiter=#{opts[10]}"
    sysprops << "-Dtd.bulk_import.prepare_parts.newline=#{opts[11]}"
    sysprops << "-Dtd.bulk_import.prepare_parts.column-header=#{opts[12]}" if opts[3]
    sysprops << "-Dtd.bulk_import.prepare_parts.columns=#{opts[13]}" if opts[1]
    sysprops << "-Dtd.bulk_import.prepare_parts.column-types=#{opts[14]}" if opts[2]
    sysprops << "-Dtd.bulk_import.prepare_parts.type-conversion-error=#{opts[15]}" if opts[15]
    sysprops << "-Dtd.bulk_import.prepare_parts.exclude-columns=#{opts[16]}"
    sysprops << "-Dtd.bulk_import.prepare_parts.only-columns=#{opts[17]}"

    sysprops << "-Dtd.bulk_import.upload_parts.auto-perform=#{opts[18]}"
    sysprops << "-Dtd.bulk_import.upload_parts.auto-commit=#{opts[19]}"
    sysprops << "-Dtd.bulk_import.upload_parts.parallel=#{opts[20]}"
    sysprops << "-Dtd.bulk_import.upload_parts.retrycount=10"
    sysprops << "-Dtd.bulk_import.upload_parts.waitsec=1"
    sysprops << "-Dtd.api.key=#{get_client.apikey}"
    sysprops
  end

  private
  def prepare_parts2_config(op)
    format = 'csv'
    compress = 'none'
    encoding = 'utf-8'
    time_column = 'time'
    time_format = nil
    time_value = nil
    outdir = nil
    split_size_kb = PART_SPLIT_SIZE / 1024  # kb
    error_record_output = nil
    dry_run = false

    delimiter = ','
    newline = 'CRLF'
    column_header = nil
    columns = nil
    column_types = nil
    type_conversion_error = 'skip'
    exclude_columns = nil
    only_columns = nil

    # prepare_parts
    op.on('-f', '--format NAME', 'source file format [csv, tsv]; default=csv') {|s|
      format = s
    }
    op.on('-C', '--compress TYPE', 'compressed type [gzip, none]; default=auto detect') {|s|
      compress = s
    }
    op.on('-e', '--encoding TYPE', 'encoding type [utf-8]') {|s|
      encoding = s
    }
    op.on('-t', '--time-column NAME', 'name of the time column') {|s|
      time_column = s
    }
    op.on('-T', '--time-format', 'STRF_FORMAT; default=auto detect') {|s|
      time_format = s
    }
    op.on('--time-value TIME', 'long value of the time column') {|s|
      if s.to_i.to_s == s
        time_value = s.to_i
      else
        require 'time'
        time_value = Time.parse(s).to_i
      end
    }
    op.on('-o', '--output DIR', 'output directory') {|s|
      outdir = s
    }
    op.on('-s', '--split-size SIZE_IN_KB', "size of each parts (default: #{split_size_kb})", Integer) {|i|
      split_size_kb = i
    }
    op.on('--error-record-output FILE', 'error records output file; default=NULL output stream') {|s|
      error_record_outdir = s
    }
    op.on('--dry-run', 'show samples as JSON and exit', FalseClass) {|b|
      dry_run = b
    }
    op.on('--delimiter CHAR', 'delimiter CHAR; default="," at csv, "\t" at tsv') {|s|
      delimiter = s
    }
    op.on('--newline', 'newline [CRLR, LR, CR];  default=CRLF') {|s|
      newline = s
    }
    op.on('-H', '--column-header', 'first line includes column names', TrueClass) {|b|
      column_header = b
    }
    op.on('-h', '--columns NAME,NAME,...', 'column names (use --column-header instead if the first line has column names)') {|s|
      columns = s
    }
    op.on('--column-types TYPE,TYPE,...', 'column types [string, int, long]') {|s|
      column_types = s
    }
    op.on('--type-conversion-error TYPE', 'type conversion error [skip,null]; default=skip') {|s|
      type_conversion_error = s
    }
    op.on('--exclude-columns NAME,NAME,...', 'exclude columns') {|s|
      exclude_columns = s
    }
    op.on('--only-columns NAME,NAME,...', 'only columns') {|s|
      only_columns = s
    }

    files = op.cmd_parse
    files = [files] unless files.is_a?(Array) # TODO ruby 1.9

    # options validation
    #unless column_types
    #  $stderr.puts "--column-types TYPE,TYPE,... option is required."
    #  exit 1
    #end
    unless outdir
      $stderr.puts "-o, --output DIR option is required."
      exit 1
    end

    return [ format, compress, encoding,
             time_column, time_format, time_value,
             outdir,  split_size_kb, error_record_output, dry_run,
             delimiter, newline, column_header, columns, column_types,
             type_conversion_error, exclude_columns, only_columns, files ]
  end

  private
  def upload_parts2_config(op)
    format = 'csv'
    compress = 'none'
    encoding = 'utf-8'
    time_column = 'time'
    time_format = nil
    time_value = nil
    outdir = nil
    split_size_kb = PART_SPLIT_SIZE / 1024  # kb
    error_record_output = nil
    dry_run = false

    delimiter = ','
    newline = 'CRLF'
    column_header = nil
    columns = nil
    column_types = nil
    type_conversion_error = 'skip'
    exclude_columns = nil
    only_columns = nil

    auto_perform = false
    auto_commit = false
    parallel = 2

    # prepare_parts
    op.on('-f', '--format NAME', 'source file format [csv, tsv]; default=csv') {|s|
      format = s
    }
    op.on('-C', '--compress TYPE', 'compressed type [gzip, none]; default=auto detect') {|s|
      compress = s
    }
    op.on('-e', '--encoding TYPE', 'encoding type [utf-8]') {|s|
      encoding = s
    }
    op.on('-t', '--time-column NAME', 'name of the time column') {|s|
      time_column = s
    }
    op.on('-T', '--time-format', 'STRF_FORMAT; default=auto detect') {|s|
      time_format = s
    }
    op.on('--time-value TIME', 'long value of the time column') {|s|
      if s.to_i.to_s == s
        time_value = s.to_i
      else
        require 'time'
        time_value = Time.parse(s).to_i
      end
    }
    op.on('-o', '--output DIR', 'output directory') {|s|
      outdir = s
    }
    op.on('-s', '--split-size SIZE_IN_KB', "size of each parts (default: #{split_size_kb})", Integer) {|i|
      split_size_kb = i
    }
    op.on('--error-record-output FILE', 'error records output file; default=NULL output stream') {|s|
      error_record_outdir = s
    }
    op.on('--dry-run', 'show samples as JSON and exit', FalseClass) {|b|
      dry_run = b
    }
    op.on('--delimiter CHAR', 'delimiter CHAR; default="," at csv, "\t" at tsv') {|s|
      delimiter = s
    }
    op.on('--newline', 'newline [CRLR, LR, CR];  default=CRLF') {|s|
      newline = s
    }
    op.on('-H', '--column-header', 'first line includes column names', TrueClass) {|b|
      column_header = b
    }
    op.on('-h', '--columns NAME,NAME,...', 'column names (use --column-header instead if the first line has column names)') {|s|
      columns = s
    }
    op.on('--column-types TYPE,TYPE,...', 'column types [string, int, long]') {|s|
      column_types = s
    }
    op.on('--type-conversion-error TYPE', 'type conversion error [skip,null]; default=skip') {|s|
      type_conversion_error = s
    }
    op.on('--exclude-columns NAME,NAME,...', 'exclude columns') {|s|
      exclude_columns = s
    }
    op.on('--only-columns NAME,NAME,...', 'only columns') {|s|
      only_columns = s
    }
    # upload_parts
    op.on('--auto-perform', 'perform bulk import job automatically', TrueClass) {|b|
      auto_perform = b
    }
    op.on('--auto-commit', 'commit bulk import job automatically', FalseClass) {|b|
      auto_commit = b
    }
    op.on('--parallel NUM', 'upload in parallel (default: 2; max 8)', Integer) {|i|
      parallel = i
    }

    files = op.cmd_parse
    files = [files] unless files.is_a?(Array) # TODO ruby 1.9

    # options validation
    #unless column_types
    #  $stderr.puts "--column-types TYPE,TYPE,... option is required."
    #  exit 1
    #end
    unless outdir
      $stderr.puts "-o, --output DIR option is required."
      exit 1
    end

    return [ format, compress, encoding,
             time_column, time_format, time_value,
             outdir,  split_size_kb, error_record_output, dry_run,
             delimiter, newline, column_header, columns, column_types,
             type_conversion_error, exclude_columns, only_columns,
             auto_perform, auto_commit, parallel, files ]
  end

  private
  def bulk_import_upload_impl(name, part_name, io, size, retry_limit, retry_wait)
    retry_count = 0
    begin
      client = get_client
      client.bulk_import_upload_part(name, part_name, io, size)
    rescue
      if retry_limit >= retry_count
        retry_count += 1
        $stderr.write "#{$!}; retrying '#{part_name}'...\n"
        w = (retry_count ** 1.5) * retry_wait
        sleep w
        retry
      end
      raise
    end
  end
end
end

