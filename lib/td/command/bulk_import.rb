
module TreasureData
module Command

  def bulk_import_list(op)
    op.cmd_parse

    bis = client.bulk_imports

    rows = []
    bis.each {|bi|
      bi << {:Name=>bi.name, :Table=>"#{bi.database}.#{bi.table}", :Status=>bi.status, :Frozen=>bi.upload_frozen?, :JobID=>bi.job_id, :"Valid Records"=>bi.valid_records, :"Error Records"=>bi.error_records}
    }

    puts cmd_render_table(rows, :fields => [:Name, :Table, :Status, :Frozen, :JobID, :"Valid Records", :"Error Records"])

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

    $stderr.puts "Name         : #{bi.name}"
    $stderr.puts "Database     : #{bi.database}"
    $stderr.puts "Table        : #{bi.table}"
    $stderr.puts "Status       : #{bi.status}"
    $stderr.puts "Frozen       : #{bi.upload_frozen?}"
    $stderr.puts "JobID        : #{bi.job_id}"
    $stderr.puts "Valid Records: #{bi.valid_records}"
    $stderr.puts "Error Records: #{bi.error_records}"
    $stderr.puts "Valid Parts  : #{bi.valid_parts}"
    $stderr.puts "Error Parts  : #{bi.error_parts}"
    $stderr.puts "Uploaded Parts :"

    client.list_bulk_import_parts.each {|part|
      puts part
    }
  end

  def bulk_import_upload(op)
    name, part_name, path = op.cmd_parse

    client = get_client

    File.open(path) {|is|
      client.upload_bulk_import(name, part_name, is, is.size)
    }

    $stderr.puts "Part '#{part_name}' is uploaded."
  end

  def bulk_import_perform(op)
    verbose = nil
    wait = false

    op.on('-v', '--verbose', 'show logs', TrueClass) {|b|
      verbose = b
    }
    op.on('-w', '--wait', 'wait for finishing the job', TrueClass) {|b|
      wait = b
    }

    name = op.cmd_parse

    client = get_client

    job = client.perform_bulk_import(name)

    $stderr.puts "Job #{job.job_id} is queued."
    $stderr.puts "Use '#{$prog} job:show #{job.job_id}' to show the status."

    if wait
      wait_job(job)
    end

    $stderr.puts "Use '-v' option to show detailed messages." unless verbose
  end

  def bulk_import_commit(op)
    name = op.cmd_parse

    client = get_client

    job = client.commit_bulk_import(name)

    $stderr.puts "Bulk import session '#{name}' started to commit."
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

    client.freeze_bulk_import(name)

    $stderr.puts "Bulk import session '#{name}' is unfrozen."
  end

end
end

