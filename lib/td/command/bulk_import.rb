require 'td/command/job'

module TreasureData
module Command

  def bulk_import_list(op)
    set_render_format_option(op)

    op.cmd_parse

    client = get_client

    bis = client.bulk_imports

    rows = []
    bis.each {|bi|
      rows << {:Name=>bi.name, :Table=>"#{bi.database}.#{bi.table}", :Status=>bi.status.to_s.capitalize, :Frozen=>bi.upload_frozen? ? 'Frozen' : '', :JobID=>bi.job_id, :"Valid Parts"=>bi.valid_parts, :"Error Parts"=>bi.error_parts, :"Valid Records"=>bi.valid_records, :"Error Records"=>bi.error_records}
    }

    $stdout.puts cmd_render_table(rows, :fields => [:Name, :Table, :Status, :Frozen, :JobID, :"Valid Parts", :"Error Parts", :"Valid Records", :"Error Records"], :max_width=>200, :render_format => op.render_format)

    if rows.empty?
      $stderr.puts "There are no bulk import sessions."
      $stderr.puts "Use '#{$prog} " + Config.cl_options_string + "bulk_import:create <name> <db> <table>' to create a session."
    end
  end

  def bulk_import_create(op)
    name, db_name, table_name = op.cmd_parse

    client = get_client
    client.create_bulk_import(name, db_name, table_name, {})

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

    bi = client.bulk_import(name)
    unless bi
      $stderr.puts "Bulk import session '#{name}' does not exist."
      $stderr.puts "Use '#{$prog} " + Config.cl_options_string + "bulk_import:create <name> <db> <table>' to create a session."
      exit 1
    end

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
      $stdout.puts "  #{name}"
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
    pool_name = nil

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
    op.on('-O', '--pool-name NAME', 'specify resource pool by name') {|s|
      pool_name = s
    }

    name, *files = op.cmd_parse

    # validate the session
    bi = get_client.bulk_imports.find {|bi| name == bi.name }
    unless bi
      $stderr.puts "Bulk import session '#{name}' does not exist. Please check the first argument of the command."
      exit 1
    end

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
      opts = {}
      opts['pool_name'] = pool_name if pool_name
      client = get_client
      job = client.perform_bulk_import(name, opts)

      $stderr.puts "Job #{job.job_id} is queued."
      $stderr.puts "Use '#{$prog} " + Config.cl_options_string + "job:show [-w] #{job.job_id}' to show the status."
    end
  end

  def bulk_import_upload_parts2(op)
    $stdout.puts "This command is moved to 'td import:upload' since 0.10.85."
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
    pool_name = nil

    op.on('-w', '--wait', 'wait for finishing the job', TrueClass) {|b|
      wait = b
    }
    op.on('-f', '--force', 'force start performing', TrueClass) {|b|
      force = b
    }
    op.on('-O', '--pool-name NAME', 'specify resource pool by name') {|s|
      pool_name = s
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
          $stderr.puts "Use '#{$prog} " + Config.cl_options_string + "job:kill #{bi.job_id}' to cancel the last trial."
          exit 1
        elsif bi.status == 'ready'
          $stderr.puts "Bulk import session '#{name}' is already ready to commit."
          $stderr.puts "Add '-f' option to force start."
          exit 1
        end
      end
    end

    opts = {}
    opts['pool_name'] = pool_name if pool_name
    job = client.perform_bulk_import(name, opts)

    $stderr.puts "Job #{job.job_id} is queued."
    $stderr.puts "Use '#{$prog} " + Config.cl_options_string + "job:show [-w] #{job.job_id}' to show the status."

    if wait
      require 'td/command/job'  # wait_job
      wait_job(job)
    end
  end

  def bulk_import_commit(op)
    wait = false

    op.on('-w', '--wait', 'wait for finishing the commit', TrueClass) {|b|
      wait = b
    }

    name = op.cmd_parse

    client = get_client

    job = client.commit_bulk_import(name)

    $stderr.puts "Bulk import session '#{name}' started to commit."

    if wait
      wait_commit(name) # wait the commit
    end
  end

  private
  def wait_commit(name)
    first_call = true
    finished = false
    max_error_counts = 10

    while first_call || !finished
      first_call = false
      begin
        sleep 5

        # extract buik_import session
        client = get_client
        bis = client.bulk_imports
        bi = bis.find {|bi| name == bi.name }
        unless bi
          $stderr.puts "Bulk import session '#{name}' does not exist."
          exit 1
        end

        # check status of the bulk_import session
        if bi.status == 'committed' || bi.status == 'ready'
          finished = true
        end
      rescue Timeout::Error, SystemCallError, EOFError, SocketError
        if max_error_counts <= 0
          raise
        end
        max_error_counts -= 1
        retry
      end
    end
  end

  def bulk_import_error_records(op)
    name = op.cmd_parse

    client = get_client

    bis = client.bulk_imports
    bi = bis.find {|bi| name == bi.name }
    unless bi
      $stderr.puts "Bulk import session '#{name}' does not exist."
      $stderr.puts "Use '#{$prog} " + Config.cl_options_string + "bulk_import:create <name> <db> <table>' to create a session."
      exit 1
    end

    if bi.status == "uploading" || bi.status == "performing"
      $stderr.puts "Bulk import session '#{name}' is not performed."
      $stderr.puts "Use '#{$prog} " + Config.cl_options_string + "bulk_import:perform <name>' to run."
      exit 1
    end

    require 'yajl'
    client.bulk_import_error_records(name) {|r|
      $stdout.puts Yajl.dump(r)
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
                      TreasureData::API.normalized_msgpack(record)
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

  def bulk_import_prepare_parts2(op)
    $stdout.puts "This command is moved to 'td import:prepare' since 0.10.85."
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

