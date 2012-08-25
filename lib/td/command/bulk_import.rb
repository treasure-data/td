
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

    puts cmd_render_table(rows, :fields => (has_org ? [:Organization] : [])+[:Name, :Table, :Status, :Frozen, :JobID, :"Valid Parts", :"Error Parts", :"Valid Records", :"Error Records"], :max_width=>200)

    if rows.empty?
      $stderr.puts "There are no bulk import sessions."
      $stderr.puts "Use '#{$prog} bulk_import:create <name> <db> <table>' to create a session."
    end
  end

  def bulk_import_create(op)
    name, db_name, table_name = op.cmd_parse

    client = get_client

    table = get_table(client, db_name, table_name)

    client.create_bulk_import(name, db_name, table_name)

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

  def bulk_import_upload_part(op)
    name, part_name, path = op.cmd_parse

    client = get_client

    File.open(path, "rb") {|is|
      client.bulk_import_upload_part(name, part_name, is, is.size)
    }

    $stderr.puts "Part '#{part_name}' is uploaded."
  end

  def bulk_import_delete_part(op)
    name, part_name = op.cmd_parse

    client = get_client

    client.bulk_import_delete_part(name, part_name)

    $stderr.puts "Part '#{part_name}' is deleted."
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
    $stderr.puts "Use '#{$prog} job:show #{job.job_id}' to show the status."

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

    client.bulk_import_error_records(name) {|r|
      puts r.to_json
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

  def bulk_import_prepare_part(op)
    outdir = nil

    require 'td/file_reader'
    reader = FileReader.new
    reader.init_optparse(op)

    op.on('-o', '--output DIR', 'output directory') {|s|
      outdir = s
    }

    *files = op.cmd_parse

    unless outdir
      $stderr.puts "-o, --output DIR option is required."
      exit 1
    end

    require 'fileutils'
    FileUtils.mkdir_p(outdir)

    require 'json'
    require 'msgpack'
    require 'zlib'

    error = Proc.new {|reason,data|
      begin
        STDERR.puts "#{reason}: #{data.to_json}"
      rescue
        STDERR.puts "#{reason}"
      end
    }

    files.each {|ifname|
      puts "Processing #{ifname}..."
      record_num = 0

      basename = File.basename(ifname).split('.').first
      File.open(ifname) {|io|
        of_index = 0
        out = nil
        zout = nil
        begin
          reader.parse(io, error) {|record|
            if zout == nil
              ofname = "#{basename}_#{of_index}.msgpack.gz"
              puts "  #{ifname} -> #{ofname}"
              out = File.open("#{outdir}/#{ofname}", 'wb')
              zout = Zlib::GzipWriter.new(out)
            end

            zout.write(record.to_msgpack)
            record_num += 1

            if out.size > PART_SPLIT_SIZE
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
        puts "  #{ifname}: #{record_num} entries."
      }
    }
  end

end
end

